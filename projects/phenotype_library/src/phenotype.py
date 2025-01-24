from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional
from enum import Enum
import pandas as pd

class VocabularyType(Enum):
    """
    Supported vocabulary types
    """
    SNOMED = "SNOMED-CT"
    ICD10 = "ICD-10"
    BNF = "BNF"
    DMD = "DM+D"

class PhenotypeSource(Enum):
    """
    Sources of concept definitions
    """
    HDRUK = "HDR UK Library"
    LONDON = "OneLondon Terminology Server"
    ICB_NEL = "North East London ICB Local Definition"

@dataclass
class Code:
    # concept level (See README.md)
    concept_code: str
    concept_name: str

@dataclass
class Codelist:
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

    def to_dataframe(self):
        phenotype_records = []
        for codelist in cls.codelists:
            for code in codelist:
                record = {
                    'CONCEPT_CODE': code.concept_code,
                    'CONCEPT_NAME': code.concept_name,
                    'VOCABULARY': codelist.vocabulary.value,
                    'CODELIST_ID': codelist.codelist_id,
                    'CODELIST_NAME': codelist.codelist_name,
                    'CODELIST_VERSION': codelist.codelist_version,
                    'PHENOTYPE_ID': self.phenotype_id,
                    'PHENOTYPE_NAME': self.phenotype_name,
                    'PHENOTYPE_VERSION': self.phenotype_version,
                    'PHENOTYPE_SOURCE': self.phenotype_source.value,
                    'OMOP_CONCEPT_ID': self.omop_concept_id,
                    'VERSION_DATETIME': self.version_datetime,
                    'UPLOADED_DATETIME': self.uploaded_datetime,
                    }
                phenotype_records.append(record)
                
        return pd.DataFrame(phenotype_records)