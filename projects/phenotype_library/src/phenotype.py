from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional
from enum import Enum
import pandas as pd
from pprint import pprint

class VocabularyType(Enum):
    """
    Supported vocabulary types
    """
    SNOMED = "SNOMED" #SNOMED-CT
    ICD10 = "ICD10"
    BNF = "BNF"
    DMD = "DMD" #DM+D

class PhenotypeSource(Enum):
    """
    Sources of concept definitions
    """
    HDRUK = "HDRUK" #HDR UK Library
    LONDON = "LONDON" #One London terminology server
    ICB_NEL = "ICB_NEL" #North East London ICB Local Definition

"""
This dictionary deals with the problem that different APIs return different strings for their vocabularies and we want to standardise things 
"""
vocab_mappings = {
    'SNOMED  CT codes': VocabularyType.SNOMED
}

@dataclass
class Code:
    """
    Class to store individual codes and descriptions e.g. Code(code='49455004', code_description='Polyneuropathy co-occurrent and due to diabetes mellitus (disorder)')
    """
    # concept level (See README.md)
    code: str
    code_description: str

@dataclass
class Codelist:
    """
    Class to store lists of codes
    """
    # codelist level (See REAME.md)
    vocabulary: VocabularyType
    codelist_id: str
    codelist_name: str
    codelist_version: str
    codes: list[Code]

@dataclass
class Phenotype:
    """
    Representation of Phenotype components that captures concept, codelist and phenotype relationships
    Used to map directly to database tables while maintaining hierarchical information
    """
    # phenotype level (See README.md)
    phenotype_id: str
    phenotype_name: str
    phenotype_version: str
    phenotype_source: PhenotypeSource
    codelists: list[Codelist]
    # omop for reference
    omop_concept_id: Optional[int] = None
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
                    'code': code.code,
                    'code_description': code.code_description,
                    'vocabulary': codelist.vocabulary.value,
                    'codelist_id': codelist.codelist_id,
                    'codelist_name': codelist.codelist_name,
                    'codelist_version': codelist.codelist_version,
                    'phenotype_id': self.phenotype_id,
                    'phenotype_name': self.phenotype_name,
                    'phenotype_version': self.phenotype_version,
                    'phenotype_source': self.phenotype_source.value,
                    'omop_concept_id': self.omop_concept_id,
                    'version_datetime': self.version_datetime,
                    'uploaded_datetime': self.uploaded_datetime,
                    }
                phenotype_records.append(record)
                
        return pd.DataFrame(phenotype_records)
    
    @classmethod
    def from_dataframe(cls, input_df):
        """
        Using this method, you can define a phenotype object from a dataframe. 
        Note that the vocabulary type and phenotype source should just be strings - they will be mapped to equivalent Enums
        """
        expected_columns = {
            'code',
            'code_description',
            'vocabulary',
            'codelist_id',
            'codelist_name',
            'codelist_version',
            'phenotype_id',
            'phenotype_name',
            'phenotype_version',
            'phenotype_source',
        }
        input_columns = set(input_df.columns)
        if not expected_columns.issubset(input_columns):
            ValueError(f'Wrong columns in input. You gave {input_columns} and not {expected_columns}')
        else:
            phenotype_id = input_df['phenotype_id'].iloc[0] #should break if more than one unique value (and for all below)
            phenotype_name = input_df['phenotype_name'].iloc[0]
            phenotype_version = input_df['phenotype_version'].iloc[0]
            phenotype_source = PhenotypeSource[input_df['phenotype_source'].iloc[0]]

            codelists = []
            for codelist_id, codelist_df in input_df.groupby('codelist_id'):
                codes = [Code(code=row['code'], code_description=row['code_description']) for _, row in codelist_df.iterrows()]
                codelist = Codelist(codes=codes, 
                                    codelist_id=codelist_id, 
                                    codelist_name=codelist_df['codelist_name'].iloc[0],
                                    codelist_version=codelist_df['codelist_version'].iloc[0],
                                    vocabulary=vocab_mappings[codelist_df['vocabulary'].iloc[0]])
                codelists.append(codelist)

            # NB need to deal with     omop_concept_id, version_datetime, uploaded_datetime
            # Need to extract them from the dataframe if exist and otherwise return none

        return cls(phenotype_id=phenotype_id, 
                   phenotype_name=phenotype_name, 
                   phenotype_version=phenotype_version, 
                   phenotype_source=phenotype_source, 
                   codelists=codelists)
    
    def show(self):
        pprint(self.df)
