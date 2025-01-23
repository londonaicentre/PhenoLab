from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Dict, Optional, Union, Literal
from enum import Enum

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
    NHSE = "NHS England Terminology"
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
    omop_concept_id: Optional[int] = None
    code_description: str
    valid_from: Optional[datetime] = None
    valid_to: Optional[datetime] = None
    # codelist level (See REAME.md)
    codelist_id: str
    codelist_name: str
    codelist_version: str
    vocabulary: VocabularyType
    # phenotype level (See README.md)
    phenotype_id: str
    phenotype_name: str
    phenotype_version: str
    phenotype_source: PhenotypeSource

