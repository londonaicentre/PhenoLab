import datetime
import hashlib
import json
import os
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional, Tuple, Union

@dataclass
class UnitConversion:
    """
    Represents a conversion formula from one unit to another.
    The conversion follows: (((value + pre_offset) * multiply_by) + add_offset)
    """
    convert_from_unit: str
    convert_to_unit: str
    pre_offset: float = 0.0   # addition offset (applied before multiplication)
    multiply_by: float = 1.0  # multiplication factor
    post_offset: float = 0.0   # addition offset (applied after multiplication)

@dataclass
class UnitMapping:
    """
    Represents a mapping from a source unit to a 'standard unit'.
    """
    source_unit: str
    standard_unit: str
    source_unit_count: int = 0
    source_unit_lq: Optional[float] = None
    source_unit_median: Optional[float] = None
    source_unit_uq: Optional[float] = None

@dataclass
class MeasurementConfig:
    """
    Represents the main standard measurement configuration
    """
    definition_id: str
    definition_name: str
    standard_units: List[str] = field(default_factory=list)  # list of standard units onto which sources will map
    primary_standard_unit: Optional[str] = None  # the standard unit used as final target for conversion
    unit_mappings: List[UnitMapping] = field(default_factory=list)  # list of source to standard mappings
    unit_conversions: List[UnitConversion] = field(default_factory=list)  # list of conversion formulas onto target
    created_datetime: str = field(default_factory=lambda: datetime.datetime.now().isoformat())
    updated_datetime: Optional[str] = None
    standard_measurement_config_id: Optional[str] = None
    standard_measurement_config_version: Optional[str] = None
    _modified: bool = field(default=True) # track if updates made for version change

    def __post_init__(self):
        if self.updated_datetime is None:
            self.updated_datetime = self.created_datetime

        # if new, create ID tied to definition
        if self.standard_measurement_config_id is None:
            content = f"{self.definition_name}_{self.definition_id}"
            self.standard_measurement_config_id = hashlib.md5(content.encode()).hexdigest()[:8]

        # create initial version if not provided
        if self.standard_measurement_config_version is None:
            self._update_version()

    def _update_version(self):
        """
        Update version when configuration has been updated
        """
        timestamp = datetime.datetime.now()
        self.standard_measurement_config_version = f"{self.definition_name}_{timestamp.strftime('%Y%m%d_%H%M%S')}"
        self._modified = False

    def mark_modified(self):
        """
        Marks as modified to execute _update_version on save
        """
        self._modified = True

    def add_standard_unit(self, unit: str) -> bool:
        """
        Add a standard unit if it doesn't already exist
        """
        if unit in self.standard_units:
            return False

        self.standard_units.append(unit)
        self.mark_modified()
        return True

    def remove_standard_unit(self, unit: str) -> bool:
        """
        Remove a standard unit if it exists
        """
        if unit not in self.standard_units:
            return False

        # check if primary standard unit
        if unit == self.primary_standard_unit:
            self.primary_standard_unit = None

        # remove mappings that use this unit
        self.unit_mappings = [m for m in self.unit_mappings if m.standard_unit != unit]
        self.unit_conversions = [c for c in self.unit_conversions
                                 if c.convert_from_unit != unit and c.convert_to_unit != unit]

        self.standard_units.remove(unit)
        self.mark_modified()
        return True

    def set_primary_standard_unit(self, unit: str) -> bool:
        """
        Set the primary standard unit for conversions
        """
        if unit not in self.standard_units:
            return False

        self.primary_standard_unit = unit
        self.mark_modified()
        return True

    def add_unit_mapping(self,
                         source_unit: str,
                         standard_unit: str,
                         count: int = 0,
                         lq: float = None,
                         median: float = None,
                         uq: float = None) -> bool:
        """
        Add or update a mapping from source unit to standard unit
        """
        if standard_unit not in self.standard_units:
            return False

        # remove existing mapping if one exists
        self.unit_mappings = [m for m in self.unit_mappings if m.source_unit != source_unit]

        # add new mapping
        mapping = UnitMapping(
            source_unit=source_unit,
            standard_unit=standard_unit,
            source_unit_count=count,
            source_unit_lq=lq,
            source_unit_median=median,
            source_unit_uq=uq
        )

        self.unit_mappings.append(mapping)
        self.mark_modified()
        return True

    def add_unit_conversion(self,
                            convert_from_unit: str,
                            convert_to_unit: str,
                            pre_offset: float = 0.0,
                            multiply_by: float = 1.0,
                            post_offset: float = 0.0) -> bool:
        """
        Add or update a conversion between standard units
        """
        if convert_from_unit not in self.standard_units or convert_to_unit not in self.standard_units:
            return False

        # remove conversion if already exists
        self.unit_conversions = [c for c in self.unit_conversions if
                              not (c.convert_from_unit == convert_from_unit and c.convert_to_unit == convert_to_unit)]

        # overwrite with new conversion
        conversion = UnitConversion(
            convert_from_unit=convert_from_unit,
            convert_to_unit=convert_to_unit,
            pre_offset=pre_offset,
            multiply_by=multiply_by,
            post_offset=post_offset
        )

        self.unit_conversions.append(conversion)
        self.mark_modified()
        return True

    def to_dict(self) -> dict:
        """
        Convert measurement config to dictionary for json
        """
        return {
            "definition_id": self.definition_id,
            "definition_name": self.definition_name,
            "standard_units": self.standard_units,
            "primary_standard_unit": self.primary_standard_unit,
            "unit_mappings": [
                {
                    "source_unit": m.source_unit,
                    "standard_unit": m.standard_unit,
                    "source_unit_count": m.source_unit_count,
                    "source_unit_lq": m.source_unit_lq,
                    "source_unit_median": m.source_unit_median,
                    "source_unit_uq": m.source_unit_uq
                }
                for m in self.unit_mappings
            ],
            "unit_conversions": [
                {
                    "convert_from_unit": c.convert_from_unit,
                    "convert_to_unit": c.convert_to_unit,
                    "pre_offset": c.pre_offset,
                    "multiply_by": c.multiply_by,
                    "post_offset": c.post_offset
                }
                for c in self.unit_conversions
            ],
            "created_datetime": self.created_datetime,
            "updated_datetime": self.updated_datetime,
            "standard_measurement_config_id": self.standard_measurement_config_id,
            "standard_measurement_config_version": self.standard_measurement_config_version
        }

    def save_to_json(self, directory: str = "data/measurements") -> str:
        """
        Save measurement config to json and update version if modified
        """
        os.makedirs(directory, exist_ok=True)

        # updates if modified
        if self._modified:
            self.updated_datetime = datetime.datetime.now().isoformat()
            self._update_version()

        # save
        filename = f"standard_{self.definition_name}_{self.standard_measurement_config_id}.json"
        filepath = os.path.join(directory, filename)

        with open(filepath, "w") as f:
            json.dump(self.to_dict(), f, indent=2)

        return filepath


def measurement_config_from_dict(data: dict) -> MeasurementConfig:
    """
    Create a MeasurementConfig object from a dictionary
    """
    config = MeasurementConfig(
        definition_id=data["definition_id"],
        definition_name=data["definition_name"],
        standard_units=data.get("standard_units", []),
        primary_standard_unit=data.get("primary_standard_unit"),
        created_datetime=data.get("created_datetime"),
        updated_datetime=data.get("updated_datetime"),
        standard_measurement_config_id=data.get("standard_measurement_config_id"),
        standard_measurement_config_version=data.get("standard_measurement_config_version"),
        _modified=False,  # fresh load
    )

    # source -> standard mappings
    for mapping_data in data.get("unit_mappings", []):
        mapping = UnitMapping(
            source_unit=mapping_data["source_unit"],
            standard_unit=mapping_data["standard_unit"],
            source_unit_count=mapping_data.get("source_unit_count", 0),
            source_unit_lq=mapping_data.get("source_unit_lq"),
            source_unit_median=mapping_data.get("source_unit_median"),
            source_unit_uq=mapping_data.get("source_unit_uq")
        )
        config.unit_mappings.append(mapping)

    # standard -> target stanrad conversions
    for conversion_data in data.get("unit_conversions", []):
        conversion = UnitConversion(
            convert_from_unit=conversion_data["convert_from_unit"],
            convert_to_unit=conversion_data["convert_to_unit"],
            pre_offset=conversion_data.get("pre_offset", 0.0),
            multiply_by=conversion_data.get("multiply_by", 1.0),
            post_offset=conversion_data.get("post_offset", 0.0)
        )
        config.unit_conversions.append(conversion)

    return config

def load_measurement_config_from_json(filepath: str) -> MeasurementConfig:
    """
    Load MeasurementConfig from json
    """
    with open(filepath, "r") as f:
        data = json.load(f)

    return measurement_config_from_dict(data)