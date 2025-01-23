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
class Phenotype:
    """
    Representation of Phenotype components that captures concept, codelist and phenotype relationships
    Used to map directly to database tables while maintaining hierarchical information
    """
    # concept level (See README.md)
    concept_code: str
    concept_name: str
    # codelist level (See REAME.md)
    vocabulary: VocabularyType
    codelist_id: str
    codelist_name: str
    codelist_version: str
    # phenotype level (See README.md)
    phenotype_id: str
    phenotype_name: str
    phenotype_version: str
    phenotype_source: PhenotypeSource
    # omop for reference
    omop_concept_id: Optional[int] = None
    # validity
    version_datetime: Optional[datetime] = None
    uploaded_datetime: Optional[datetime] = None

    @classmethod
    def to_dataframe(cls, phenotypes: List['Phenotype']) -> pd.DataFrame:
        """
        Convert a list of Phenotype objects to a pandas DataFrame
        """
        phenotype_records = []
        for p in phenotypes:
            record = {
                'CONCEPT_CODE': p.concept_code,
                'CONCEPT_NAME': p.concept_name,
                'VOCABULARY': p.vocabulary.value,
                'CODELIST_ID': p.codelist_id,
                'CODELIST_NAME': p.codelist_name,
                'CODELIST_VERSION': p.codelist_version,
                'PHENOTYPE_ID': p.phenotype_id,
                'PHENOTYPE_NAME': p.phenotype_name,
                'PHENOTYPE_VERSION': p.phenotype_version,
                'PHENOTYPE_SOURCE': p.phenotype_source.value,
                'OMOP_CONCEPT_ID': p.omop_concept_id,
                'VERSION_DATETIME': p.version_datetime,
                'UPLOADED_DATETIME': p.uploaded_datetime,
            }
            phenotype_records.append(record)

        return pd.DataFrame(phenotype_records)