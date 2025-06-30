from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pprint import pprint
from typing import Optional, Self
import boto3
from boto3.dynamodb.conditions import Key
from dotenv import load_dotenv
import hashlib
import json
import os

import pandas as pd


class VocabularyType(Enum):
    """
    Supported vocabulary types
    These match scheme naming in NEL ICB Compass
    """

    SNOMED = "SNOMED"  # SNOMED-CT
    ICD10 = "ICD10"
    BNF = "BNF"
    READV2 = "READ 2"
    READV3 = "READ 3"
    DMD = "DM+D code scheme"  # DM+D
    MEDCODE = "MEDCODE"  # HDRUK CPRD 'Med Code' - these do not appear in London vocabulary
    OPCS4 = "OPCS4"


class DefinitionSource(Enum):
    """
    Sources of concept definitions
    """

    HDRUK = "HDRUK"  # HDR UK Library
    LONDON = "LONDON"  # One London terminology server
    ICB_NEL = "ICB_NEL"  # North East London ICB Local Definition
    NHSBSA = "NHSBSA"  # NHS Business Services Authority
    OPEN_CODELISTS = "OPEN_CODELISTS"  # OpenCodelists.org
    AICENTRE = "AICENTRE"


"""
This dictionary deals with the problem that different APIs return different strings for their
vocabularies and we want to standardise things
"""
vocab_mappings = {
    "SNOMED CT codes": VocabularyType.SNOMED,  # HDRUK
    "ICD10 codes": VocabularyType.ICD10,  # HDRUK
    "Read codes v2": VocabularyType.READV2,  # HDRUK
    "Med codes": VocabularyType.MEDCODE,  # HDRUK
    "BNF codes": VocabularyType.BNF,  # HDRUK
    "SNOMED CT": VocabularyType.SNOMED,  # Open Codelists
    "SNOMED CT (UK Clinical Edition)": VocabularyType.SNOMED,  # Open Codelists
    "Read V2": VocabularyType.READV2,  # Open Codelists
    "CTV3 (Read V3)": VocabularyType.READV3,  # Open Codelists
    "ICD-10": VocabularyType.ICD10,  # Open Codelists
    "Dictionary of Medicines and Devices": VocabularyType.DMD,  # Open Codelists
    "SNOMED": VocabularyType.SNOMED,  # Internal
    "OPCS4": VocabularyType.OPCS4,  # Internal
    "ICD10": VocabularyType.ICD10,  # Internal
    "BNF": VocabularyType.BNF,  # Internal
    "READ 2": VocabularyType.READV2,  # Internal
    "READ 3": VocabularyType.READV3,  # Internal
    "DM+D code scheme": VocabularyType.DMD,  # Internal
    "MEDCODE": VocabularyType.MEDCODE,  # Internal
}


@dataclass
class Code:
    """
    Class to store individual codes and descriptions e.g. Code(code='49455004',
    code_description='Polyneuropathy co-occurrent and due to diabetes mellitus (disorder)')
    """

    # concept level (See README.md)
    code: str
    code_description: str
    code_vocabulary: VocabularyType


@dataclass
class Codelist:
    """
    Class to store lists of codes
    """

    # codelist level (See README.md)
    codelist_id: str
    codelist_name: str
    codelist_vocabulary: VocabularyType
    codelist_version: str
    codes: list[Code]

    def __post_init__(self):
        mismatched_codes = [
            code for code in self.codes if code.code_vocabulary != self.codelist_vocabulary
        ]
        if mismatched_codes:
            raise ValueError(
                f"All codes must have same vocabulary as codelist {self.codelist_vocabulary}. "
                f"Following codes have different vocabularies: {mismatched_codes}"
            )

    @classmethod
    def from_scratch(cls, codelist_name: str, codelist_vocabulary: VocabularyType) -> Self:
        """
        This constructor is used to generate an ID and version number when we want to create a codelist de novo rather
        than from an existing source. The content of the codelist defaults to an empty list which must be populated
        incrementally.
        """

        content = f"{codelist_name}_{datetime.now().isoformat()}"
        codelist_id = f"{codelist_name}_{hashlib.md5(content.encode()).hexdigest()[:8]}"

        codelist_version = f"{codelist_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

        return cls(
            codelist_name=codelist_name,
            codelist_id=codelist_id,
            codelist_vocabulary=codelist_vocabulary,
            codelist_version=codelist_version,
            codes=[],
        )

    def add_code(self, code: Code) -> bool:
        """
        Add a code to the codelist
        Args:
            code(Code):
                Code object to add
        Returns:
            bool:
                True if code was added, False if duplicate
        """
        for existing_code in self.codes:
            if existing_code.code == code.code:
                return False
        self.codes.append(code)
        self.__post_init__() # does verification on the vocab
        return True


@dataclass
class Definition:
    """
    Representation of Definition components that captures concept, codelist and definition
    relationships
    Used to map directly to database tables while maintaining hierarchical information
    """

    # definition level (See README.md)
    definition_id: str
    definition_name: str
    definition_version: str
    definition_source: DefinitionSource
    codelists: list[Codelist] = field(default_factory=list)

    # validity
    version_datetime: Optional[datetime] = None
    uploaded_datetime: Optional[datetime] = None

    # def __post_init__(self):
    #     self.as_df = self.to_dataframe()

    @property
    def df(self) -> pd.DataFrame:
        return self.to_dataframe()

    @property
    def codes(self) -> list[Code]:
        return [code for codelist in self.codelists for code in codelist.codes]
    
    @property
    def aslist(self) -> list[dict]:
        return self.to_list()

    def to_list(self) -> list[dict]:
        definition_records = []
        for codelist in self.codelists:
            for code in codelist.codes:
                record = {
                    "code": code.code,
                    "code_description": code.code_description,
                    "vocabulary": codelist.codelist_vocabulary.value,
                    "codelist_id": codelist.codelist_id,
                    "codelist_name": codelist.codelist_name,
                    "codelist_version": codelist.codelist_version,
                    "definition_id": self.definition_id,
                    "definition_name": self.definition_name,
                    "definition_version": self.definition_version,
                    "definition_source": self.definition_source.value,
                    "version_datetime": self.version_datetime,
                    "uploaded_datetime": self.uploaded_datetime,
                }
                definition_records.append(record)
        return definition_records

    def to_dataframe(self) -> pd.DataFrame:
        """
        Create a pandas dataframe for the definition object
        """
        return pd.DataFrame(self.aslist)

    @classmethod
    def from_dataframe(cls, input_df: pd.DataFrame) -> Self:
        """
        Using this method, you can define a definition object from a dataframe.
        Note that the vocabulary type and definition source should just be strings - they will be
        mapped to equivalent Enums
        """
        expected_columns = {
            "code",
            "code_description",
            "vocabulary",
            "codelist_id",
            "codelist_name",
            "codelist_version",
            "definition_id",
            "definition_name",
            "definition_version",
            "definition_source",
        }
        input_columns = set(input_df.columns)
        if not expected_columns.issubset(input_columns):
            ValueError(
                f"Wrong columns in input. You gave {input_columns} and not {expected_columns}"
            )
        else:
            definition_id = input_df["definition_id"].iloc[
                0
            ]  # should break if more than one unique value (and for all below)
            definition_name = input_df["definition_name"].iloc[0]
            definition_version = input_df["definition_version"].iloc[0]
            definition_source = DefinitionSource[input_df["definition_source"].iloc[0]]

            # modified HDR API to pick up dates
            version_datetime = (
                input_df["version_datetime"].iloc[0]
                if "version_datetime" in input_df.columns
                else None
            )

            codelists = []
            for codelist_id, codelist_df in input_df.groupby("codelist_id"):
                codes = [
                    Code(
                        code=row["code"],
                        code_description=row["code_description"],
                        code_vocabulary=vocab_mappings[row["vocabulary"]],
                    )
                    for _, row in codelist_df.iterrows()
                ]
                codelist = Codelist(
                    codes=codes,
                    codelist_id=codelist_id,
                    codelist_name=codelist_df["codelist_name"].iloc[0],
                    codelist_version=codelist_df["codelist_version"].iloc[0],
                    codelist_vocabulary=vocab_mappings[codelist_df["vocabulary"].iloc[0]],
                )
                codelists.append(codelist)

        return cls(
            definition_id=definition_id,
            definition_name=definition_name,
            definition_version=definition_version,
            definition_source=definition_source,
            codelists=codelists,
            version_datetime=version_datetime,
        )

    @classmethod
    def from_scratch(cls, definition_name: str, codelists: list = None) -> Self:
        """
        This constructor is used to generate an ID and version number when we want to create a definition de novo rather
        than from an existing source. If no codelists are passed in, the content of these defaults to an empty list
        which must be populated incrementally.
        """
        # uploaded_datetime = datetime.now() # this is the time of creation for locally  created definitions
        uploaded_datetime = datetime.min  # placeholder - datetime added in when uploaded to DB
        content = f"{definition_name}_{uploaded_datetime}"
        definition_id = hashlib.md5(content.encode()).hexdigest()[:8]
        # definition_version = f"{definition_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        definition_source = DefinitionSource.AICENTRE # this is hard coded currently but could be a passed in input if needed

        if codelists is None:
            codelists = [] # avoids list mutability problems

        # don't have an uploaded datetime

        instance = cls(definition_name=definition_name,
                definition_id=definition_id,
                definition_version="",
                definition_source=definition_source,
                codelists = codelists,
                uploaded_datetime=uploaded_datetime)
        instance.update_version()
        return instance

    def add_code(self, code: Code) -> bool:
        """
        Add a code to the appropriate codelist
        Args:
            code(Code):
                Code object to add
        Returns:
            bool:
                True if code was added, False if duplicate
        """
        vocabulary = code.code_vocabulary

        vocabularies_in_use = [codelist.codelist_vocabulary for codelist in self.codelists]

        if vocabulary not in vocabularies_in_use:
            # Create codelist if relevant vocabulary not in use
            codelist_name = f"{self.definition_name}_{vocabulary.value}"
            new_codelist = Codelist.from_scratch(codelist_name=codelist_name, codelist_vocabulary=vocabulary)
            self.codelists.append(new_codelist)

        for codelist in self.codelists:
            if codelist.codelist_vocabulary == vocabulary:
                for existing_code in codelist.codes:
                    if existing_code.code == code.code and existing_code.code_vocabulary == code.code_vocabulary:
                        print('Code already in codelist')
                        return False # code already exists, don't add dupe
                codelist.add_code(code) # otherwise, add code to relevant codelist

        return True

    def remove_code(self, code_to_remove: Code) -> bool:
        """
        Remove a code from the definition
        Args:
            code_to_remove(Code):
                Code object to remove
        Returns:
            bool:
                True if code removed, False otherwise
        """
        for codelist in self.codelists:
            for i, code in enumerate(codelist.codes):
                if code.code == code_to_remove.code and code.code_vocabulary == code_to_remove.code_vocabulary:
                    codelist.codes.pop(i)
                    return True

        return False

    def show(self):
        pprint(self.df)
    
    def update_version(self):
        self.definition_version = f"{self.definition_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        self.version_datetime = datetime.now()
        
    @classmethod
    def from_dict(cls, data: dict) -> Self:
        """
        Create a Definition object from dict
        IDs and versions are preserved
        """
        codelists = []
        # load codelists
        for codelist_data in data.get("codelists", []):
            codelist = Codelist(
                codelist_name=codelist_data["codelist_name"],
                codelist_id=codelist_data.get("codelist_id"),
                codelist_version=codelist_data.get("codelist_version"),
                codelist_vocabulary=VocabularyType[codelist_data["codelist_vocabulary"]],
                codes=[],
            )
            codelists.append(codelist)

            # load codes
            for code_data in codelist_data.get("codes", []):
                code = Code(
                    code=code_data["code"],
                    code_description=code_data["code_description"],
                    code_vocabulary=VocabularyType[code_data["code_vocabulary"]],
                )
                codelist.codes.append(code)

        return cls(
            definition_name=data["definition_name"],
            definition_source=DefinitionSource(data.get("definition_source")),
            definition_id=data["definition_id"],
            definition_version=data["definition_version"],
            codelists=codelists,
            version_datetime=datetime.fromisoformat(data["version_datetime"]),
            uploaded_datetime=datetime.fromisoformat(data["uploaded_datetime"]),
        )


    @classmethod
    def from_json(cls, filepath: str) -> Self:
        """
        Load Definition from json file
        """
        with open(filepath, "r") as f:
            data = json.load(f)

        return cls.from_dict(data)

    def to_dict(self) -> dict:
        """
        Convert definition to a dictionary for json
        """
        result = {
            "definition_id": self.definition_id,
            "definition_name": self.definition_name,
            "definition_version": self.definition_version,
            "definition_source": self.definition_source.value,
            "uploaded_datetime": self.uploaded_datetime.isoformat(),
            "version_datetime": self.version_datetime.isoformat(),
            "codelists": [],
        }

        for codelist in self.codelists:
            codelist_dict = {
                "codelist_id": codelist.codelist_id,
                "codelist_name": codelist.codelist_name,
                "codelist_vocabulary": codelist.codelist_vocabulary.value,
                "codelist_version": codelist.codelist_version,
                "codes": [],
            }

            for code in codelist.codes:
                codelist_dict["codes"].append(
                    {
                        "code": code.code,
                        "code_description": code.code_description,
                        "code_vocabulary": code.code_vocabulary.value,
                    }
                )

            result["codelists"].append(codelist_dict)

        return result

    def save_to_json(self, directory: str = "data/definitions") -> str:
        """
        Save definition to json and update versions if modified

        Args:
            directory(str):
                dir where json saved (default "data/definitions")
        Return:
            str:
                Path where saved (show in streamlit)
        """
        os.makedirs(directory, exist_ok=True)

        # filename = f"{self.definition_name}_{self.definition_id}.json"
        filename = f"{self.definition_version}.json"
        filepath = os.path.join(directory, filename)
        print(f"Saving json to {filepath} unless files exists and matches current definition")

        # check if file exists
        if os.path.exists(filepath):
            existing_definition = Definition.from_json(filepath)
            if self == existing_definition:
                print("Existing file with same definition found, no need to save")
                return filepath

        # update version - a version number is only created when the definition is saved to json
        self.version_datetime = datetime.now()

        with open(filepath, "w") as f:
            json.dump(self.to_dict(), f, indent=2)
        print(f"Definition saved to {filepath}")

        return filepath

    def save_to_dynamoDB(self):

        load_dotenv() # to get AWS access keys from .env file; need to replace with better authentication method

        dynamodb = boto3.resource("dynamodb", region_name='eu-west-2')
        table = dynamodb.Table("definitions")

        self.version_datetime = datetime.now()
        table.put_item(Item=self.to_dict())

        print(f"Item saved to DynamoDB table 'definitions' with ID {self.definition_id} and version {self.version_datetime}")

    @classmethod
    def load_from_dynamoDB(cls, definition_id: str) -> Self:

        load_dotenv() # to get AWS access keys from .env file; need to replace with better authentication method

        dynamodb = boto3.resource("dynamodb", region_name='eu-west-2')
        table = dynamodb.Table("definitions")

        response = table.query(KeyConditionExpression=Key('definition_id').eq(definition_id), 
                            ScanIndexForward=False, Limit=1)
        
        items = response.get("Items", [])

        if items:
            latest_item = items[0]
            print(f"Latest item found in DynamoDB with ID {latest_item['definition_id']} and version {latest_item['version_datetime']}")
            return cls.from_dict(latest_item)
        else:
            print(f"No items found in DynamoDB with ID {definition_id}")