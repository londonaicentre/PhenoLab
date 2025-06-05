import datetime
import hashlib
import json
import os
import re
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, Optional, Tuple

"""
## phenotype.py

Provides classes for representing phenotypes and their conditions, /
with support for logical expressions and validation.

This contains classes for:
- ConditionBlock
-- represents the use of a Definition with basic logic
-- For example, HAS DEFINTIION, or DEFINITION MEASUREMENT > 120mmHg
- Phenotype
-- A Phenotype chains ConditionBLocks with additional logic
-- AND/OR/NOT are valid operators between ConditionBlocks [A,B,C...]
-- The resulting Phenotype object packages up as simple expression
-- Phenotype = (A AND B) OR C
"""


class ConditionType(str, Enum):
    """
    Possible condition types in a phenotype
        HAS_DEFINITION indicates a simple inclusion criteria based on codelists
        MEASUREMENT indicates comparison against a threshold
    """

    HAS_DEFINITION = "HAS"
    MEASUREMENT = "MEASURE"


class LogicalOperator(str, Enum):
    """
    Logical operators for phenotype expressions
    """

    AND = "AND"
    OR = "OR"
    NOT = "NOT"


class ComparisonOperator(str, Enum):
    """
    Comparison operators for measurement conditions
    """

    GREATER_THAN = ">"
    LESS_THAN = "<"
    EQUAL_TO = "="
    GREATER_EQUAL = ">="
    LESS_EQUAL = "<="
    NOT_EQUAL = "!="


@dataclass
class ConditionBlock:
    """
    Represents a basic condition block used in a phenotype expression.
    Each condition uses a definition.
    Each condition is either a simple HAS_DEFINITION, or is a MEASUREMENT.
    Blocks are joined together by LogicalOperators.
    """

    label: str  # e.g., "A", "B", "C"
    definition_id: str
    definition_name: str
    definition_source: str  # Added definition source
    condition_type: ConditionType
    comparison_operator: Optional[ComparisonOperator] = None
    threshold_value: Optional[float] = None
    threshold_unit: Optional[str] = None
    number_of_measures: Optional[int] = None  # Number of measurements required
    measure_time_window_days: Optional[int] = None  # Time window in days
    value_lower_cutoff: Optional[float] = None  # Data quality: exclude values below this
    value_upper_cutoff: Optional[float] = None  # Data quality: exclude values above this

    def to_dict(self) -> dict:
        """
        Convert to dictionary
        """
        result = {
            "label": self.label,
            "definition_id": self.definition_id,
            "definition_name": self.definition_name,
            "definition_source": self.definition_source,  # Added to dictionary output
            "condition_type": self.condition_type,
        }

        if self.condition_type == ConditionType.MEASUREMENT:
            result["comparison_operator"] = self.comparison_operator
            result["threshold_value"] = self.threshold_value
            result["threshold_unit"] = self.threshold_unit
            result["number_of_measures"] = self.number_of_measures
            result["measure_time_window_days"] = self.measure_time_window_days
            result["value_lower_cutoff"] = self.value_lower_cutoff
            result["value_upper_cutoff"] = self.value_upper_cutoff

        return result

    @classmethod
    def from_dict(cls, data: dict) -> "ConditionBlock":
        """
        Create a ConditionBlock from a dict
        """
        return cls(
            label=data["label"],
            definition_id=data["definition_id"],
            definition_name=data["definition_name"],
            definition_source=data.get("definition_source", "UNKNOWN"),  # Default for backward compatibility
            condition_type=data["condition_type"],
            comparison_operator=data.get("comparison_operator"),
            threshold_value=data.get("threshold_value"),
            threshold_unit=data.get("threshold_unit"),
            number_of_measures=data.get("number_of_measures"),
            measure_time_window_days=data.get("measure_time_window_days"),
            value_lower_cutoff=data.get("value_lower_cutoff"),
            value_upper_cutoff=data.get("value_upper_cutoff"),
        )

    def to_dsl_description(self) -> str:
        """
        Return a human-readable description of this block for the UI
        """
        source_info = f" ({self.definition_source})" if self.definition_source else ""
        if self.condition_type == ConditionType.HAS_DEFINITION:
            return f"Has any code from '{self.definition_name}'{source_info}"
        else:
            base_desc = f"'{self.definition_name}'{source_info} {self.comparison_operator} {self.threshold_value} {self.threshold_unit}"
            
            # Add temporal/frequency constraints if present
            if self.number_of_measures and self.measure_time_window_days:
                return f"At least {self.number_of_measures} measurements of {base_desc} within {self.measure_time_window_days} days"
            elif self.number_of_measures:
                return f"At least {self.number_of_measures} measurements of {base_desc}"
            else:
                return base_desc


@dataclass
class Phenotype:
    """
    Represents a phenotype composed of ConditionBlocks and LogicalOperators
    """

    phenotype_name: str
    description: str
    condition_blocks: Dict[str, ConditionBlock] = field(default_factory=dict)
    expression: str = ""  # DSL expression (e.g., "(A AND B) OR C")
    created_datetime: str = field(default_factory=lambda: datetime.datetime.now().isoformat())
    updated_datetime: str = field(default=None)
    phenotype_id: str = field(default=None)
    phenotype_version: str = field(default=None)
    _modified: bool = field(default=True)  # to flag updates

    def __post_init__(self):
        """
        Initialize phenotype ID and version if not provided
        """
        # set updated_datetime if not provided
        if self.updated_datetime is None:
            self.updated_datetime = self.created_datetime

        # create ID only if new phenotype
        if self.phenotype_id is None:
            content = f"{self.phenotype_name}_{self.created_datetime}"
            self.phenotype_id = hashlib.md5(content.encode()).hexdigest()[:8]

        # create initial version if not provided
        if self.phenotype_version is None:
            self._update_version()

    def _update_version(self):
        """
        Update version, and sets modified flag to False
        Used to update version when the phenotype has been updated
        """
        timestamp = datetime.datetime.now()
        self.phenotype_version = f"{self.phenotype_name}_{timestamp.strftime('%Y%m%d_%H%M%S')}"
        self._modified = False

    def mark_modified(self):
        """
        This marks as modified
        Flag used to execute _update_version on save
        """
        self._modified = True

    def add_condition_block(
        self,
        definition_id: str,
        definition_name: str,
        definition_source: str,
        condition_type: ConditionType,
        comparison_operator: Optional[ComparisonOperator] = None,
        threshold_value: Optional[float] = None,
        threshold_unit: Optional[str] = None,
        number_of_measures: Optional[int] = None,
        measure_time_window_days: Optional[int] = None,
        value_lower_cutoff: Optional[float] = None,
        value_upper_cutoff: Optional[float] = None,
    ) -> str:
        """
        Add a new condition block to the phenotype

        Args:
            definition_id:
                ID of the definition
            definition_name:
                Name of the definition
            definition_source:
                Source of the definition (e.g., "HDRUK", "CUSTOM")
            condition_type:
                Type of condition (HAS or MEASURE)
            comparison_operator:
                Operator for measurement conditions
            threshold_value:
                Threshold for measurement conditions
            threshold_unit:
                Unit for the value being measured on
            number_of_measures:
                Number of measurements required (for temporal patterns)
            measure_time_window_days:
                Time window in days for measurement collection
            value_lower_cutoff:
                Data quality filter: exclude values below this threshold
            value_upper_cutoff:
                Data quality filter: exclude values above this threshold

        Returns:
            The label assigned to the block (e.g., "A", "B", "C")
        """
        # blocks are sequenced A, B, C etc
        if not self.condition_blocks:
            next_label = "A"
        else:
            labels = sorted(self.condition_blocks.keys())
            last_label = labels[-1]
            next_label = chr(ord(last_label) + 1)

        # create new condition block
        block = ConditionBlock(
            label=next_label,
            definition_id=definition_id,
            definition_name=definition_name,
            definition_source=definition_source,  # Added parameter
            condition_type=condition_type,
            comparison_operator=comparison_operator,
            threshold_value=threshold_value,
            threshold_unit=threshold_unit,
            number_of_measures=number_of_measures,
            measure_time_window_days=measure_time_window_days,
            value_lower_cutoff=value_lower_cutoff,
            value_upper_cutoff=value_upper_cutoff,
        )

        # add to condition blocks
        self.condition_blocks[next_label] = block
        self.mark_modified()

        return next_label

    def remove_condition_block(self, label: str) -> bool:
        """
        Remove a condition block by its label

        Returns:
            True if removed, False if not found
        """
        if label in self.condition_blocks:
            del self.condition_blocks[label]
            self.mark_modified()
            return True
        return False

    def update_expression(self, expression: str):
        """
        Update the logical expression for this phenotype
        """
        self.expression = expression  # DSL expression (e.g., "(A AND B) OR C")
        self.mark_modified()

    def validate_expression(self) -> Tuple[bool, str]:
        """
        Validate the DSL expression
        This performs simple rules based validation
        **Update in future to parse logic**

        Returns:
            tuple of (is_valid, error_message)
        """
        if not self.expression:
            return False, "Expression cannot be empty"

        # 1. Labels in DSL must equal labels of condition blocks
        valid_labels = set(self.condition_blocks.keys())
        used_labels = set(re.findall(r"\b[A-Z]\b", self.expression))

        invalid_labels = used_labels - valid_labels
        if invalid_labels:
            return False, f"Invalid labels in expression: {', '.join(invalid_labels)}"

        unused_labels = valid_labels - used_labels
        if unused_labels:
            return False, f"Unused condition blocks: {', '.join(unused_labels)}"

        # 2. Check for valid operators (AND, OR, NOT)
        operators = re.findall(r"\b(AND|OR|NOT)\b", self.expression)
        for op in operators:
            if op not in [o.value for o in LogicalOperator]:
                return False, f"Invalid operator in expression: {op}"

        # 3. Check for balanced brackets
        if not self._check_balanced_brackets(self.expression):
            return False, "Unbalanced brackets in expression"

        return True, ""

    def _check_balanced_brackets(self, expression: str) -> bool:
        """
        Check if brackets are balanced in the expression
        """
        stack = []
        for char in expression:
            if char == "(":
                stack.append(char)
            elif char == ")":
                if not stack:
                    return False
                stack.pop()
        return len(stack) == 0

    def get_expanded_expression(self) -> str:
        """
        Get a human-readable version of the expression
        Block labels (A,B,C) are replaced by desxcriptions
        """
        if not self.expression:
            return ""

        expanded = self.expression
        for label, block in self.condition_blocks.items():
            expanded = re.sub(r"\b" + label + r"\b", f"[{block.to_dsl_description()}]", expanded)

        return expanded

    def to_dict(self) -> dict:
        """
        Convert phenotype to dictionary for json
        """
        return {
            "phenotype_id": self.phenotype_id,
            "phenotype_name": self.phenotype_name,
            "phenotype_version": self.phenotype_version,
            "description": self.description,
            "created_datetime": self.created_datetime,
            "updated_datetime": self.updated_datetime,
            "condition_blocks": {label: block.to_dict() for label, block in self.condition_blocks.items()},
            "expression": self.expression,
        }

    def save_to_json(self, directory: str = "data/phenotypes") -> str:
        """
        Save phenotype to JSON and update version if modified

        Args:
            directory:
                dir where JSON will be saved

        Returns:
            Path where saved
        """
        os.makedirs(directory, exist_ok=True)

        # updates if modifified
        if self._modified:
            self.updated_datetime = datetime.datetime.now().isoformat()
            self._update_version()

        # save
        filename = f"{self.phenotype_name}_{self.phenotype_id}.json"
        filepath = os.path.join(directory, filename)

        with open(filepath, "w") as f:
            json.dump(self.to_dict(), f, indent=2)

        return filepath


def phenotype_from_dict(data: dict) -> Phenotype:
    """
    Create a Phenotype object from a dictionary
    """
    phenotype = Phenotype(
        phenotype_name=data["phenotype_name"],
        description=data["description"],
        created_datetime=data.get("created_datetime"),
        updated_datetime=data.get("updated_datetime"),
        phenotype_id=data.get("phenotype_id"),
        phenotype_version=data.get("phenotype_version"),
        expression=data.get("expression", ""),
        _modified=False,  # fresh load
    )

    # load in condition blocks
    for label, block_data in data.get("condition_blocks", {}).items():
        block = ConditionBlock.from_dict(block_data)
        phenotype.condition_blocks[label] = block

    return phenotype


def load_phenotype_from_json(filepath: str) -> Phenotype:
    """
    Load Phenotype from json
    """
    with open(filepath, "r") as f:
        data = json.load(f)

    return phenotype_from_dict(data)
