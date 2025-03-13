from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from pprint import pprint
from typing import Optional

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


class PhenotypeSource(Enum):
    """
    Sources of concept definitions
    """

    HDRUK = "HDRUK"  # HDR UK Library
    LONDON = "LONDON"  # One London terminology server
    ICB_NEL = "ICB_NEL"  # North East London ICB Local Definition
    NHSBSA = "NHSBSA"  # NHS Business Services Authority
    OPEN_CODELISTS = "OPEN_CODELISTS"  # OpenCodelists.org



"""
This dictionary deals with the problem that different APIs return different strings for their
vocabularies and we want to standardise things
"""
vocab_mappings = {
    "SNOMED  CT codes": VocabularyType.SNOMED,  # HDRUK
    "ICD10 codes": VocabularyType.ICD10,  # HDRUK
    "Read codes v2": VocabularyType.READV2,  # HDRUK
    "Med codes": VocabularyType.MEDCODE,  # HDRUK
    "BNF codes": VocabularyType.BNF,  # HDRUK
    "SNOMED CT": VocabularyType.SNOMED,  # Open Codelists
    "Read V2": VocabularyType.READV2,  # Open Codelists
    "CTV3 (Read V3)": VocabularyType.READV3,  # Open Codelists
    "ICD-10": VocabularyType.ICD10,  # Open Codelists
    "Dictionary of Medicines and Devices": VocabularyType.DMD,  # Open Codelists
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


@dataclass
class Phenotype:
    """
    Representation of Phenotype components that captures concept, codelist and phenotype
    relationships
    Used to map directly to database tables while maintaining hierarchical information
    """

    # phenotype level (See README.md)
    phenotype_id: str
    phenotype_name: str
    phenotype_version: str
    phenotype_source: PhenotypeSource
    codelists: list[Codelist]

    # validity
    version_datetime: Optional[datetime] = None
    uploaded_datetime: Optional[datetime] = None

    # def __post_init__(self):
    #     self.as_df = self.to_dataframe()

    @property
    def df(self):
        return self.to_dataframe()

    def to_dataframe(self):
        """
        Create a pandas dataframe for the phenotype object
        """
        phenotype_records = []
        for codelist in self.codelists:
            for code in codelist.codes:
                record = {
                    "code": code.code,
                    "code_description": code.code_description,
                    "vocabulary": codelist.codelist_vocabulary.value,
                    "codelist_id": codelist.codelist_id,
                    "codelist_name": codelist.codelist_name,
                    "codelist_version": codelist.codelist_version,
                    "phenotype_id": self.phenotype_id,
                    "phenotype_name": self.phenotype_name,
                    "phenotype_version": self.phenotype_version,
                    "phenotype_source": self.phenotype_source.value,
                    "version_datetime": self.version_datetime,
                    "uploaded_datetime": self.uploaded_datetime,
                }
                phenotype_records.append(record)

        return pd.DataFrame(phenotype_records)

    @classmethod
    def from_dataframe(cls, input_df):
        """
        Using this method, you can define a phenotype object from a dataframe.
        Note that the vocabulary type and phenotype source should just be strings - they will be
        mapped to equivalent Enums
        """
        expected_columns = {
            "code",
            "code_description",
            "vocabulary",
            "codelist_id",
            "codelist_name",
            "codelist_version",
            "phenotype_id",
            "phenotype_name",
            "phenotype_version",
            "phenotype_source",
        }
        input_columns = set(input_df.columns)
        if not expected_columns.issubset(input_columns):
            ValueError(
                f"Wrong columns in input. You gave {input_columns} and not {expected_columns}"
            )
        else:
            phenotype_id = input_df["phenotype_id"].iloc[
                0
            ]  # should break if more than one unique value (and for all below)
            phenotype_name = input_df["phenotype_name"].iloc[0]
            phenotype_version = input_df["phenotype_version"].iloc[0]
            phenotype_source = PhenotypeSource[input_df["phenotype_source"].iloc[0]]

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
            phenotype_id=phenotype_id,
            phenotype_name=phenotype_name,
            phenotype_version=phenotype_version,
            phenotype_source=phenotype_source,
            codelists=codelists,
            version_datetime=version_datetime,
        )

    def show(self):
        pprint(self.df)
