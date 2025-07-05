"""
Run this script to fetch definitions from the HDRUK API and save them to a parquet file in the data folder.
"""

import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

# Add the parent directory to path to access utils.definition
sys.path.append(str(Path(__file__).parent.parent))
from hdruk_api import HDRUKLibraryClient
from utils.definition import Definition

"""
Note that HDRUK refers to groupings of codelists as phenotypes
Hence each definition is referred to by a phenotype_id to interact well with the HDRUK API
"""

#######################################################
# Active definitions for retrieval from HDR-UK library #

definition_list = [
    ("PH152", 304),  # Diabetes, any (ICD10, READV2)
    ("PH189", 378),  # Hypertension (ICD10, READV2)
    ("PH717", 1434),  # Diabetes, T1 (READV2)
    ("PH753", 1506),  # Diabetes, T2 (READV2)
    ("PH88", 176),  # Transient ischaemic attack (ICD10, READV2)
    ("PH56", 112),  # Ischaemic stroke (ICD10, READV2)
    ("PH182", 364),  # Heart failure (ICD10, READV2)
    ("PH576", 1152),  # Ischaemic heart disease (ICD10, READV2)
    ("PH215", 430),  # Myocardial infarction (ICD10, READV2)
    ("PH315", 630),  # Stable angina (ICD10, READV2)
    ("PH329", 658),  # Unstable angina (ICD10, READV2)
    ("PH221", 442),  # Obesity (ICD10, READV2)
    ("PH149", 298),  # Depression (ICD10, READV2)
    ("PH104", 208),  # Anxiety Disorders (ICD10, READV2)
    ("PH285", 570),  # Schizophrenia, schizotypal, delusional (ICD10, READV2)
    ("PH94", 188),  # Alcohol problems (ICD10, READV2)
    ("PH43", 86),  # Chronic Obstructive Pulmonary Disease (ICD10, READV2)
    ("PH109", 218),  # Asthma (ICD10, READV2)
    ("PH211", 422),  # Migraine (ICD10, READV2)
    ("PH990", 2168),  # BHF Chronic kidney disease (ICD10, SNOMED)
    ("PH1018", 2196),  # BHF Stroke (ICD10, SNOMED)
    ("PH947", 2125),  # BHF Obesity (SNOMED)
    ("PH970", 2148),  # BHF Hypertension (SNOMED)
    ("PH956", 2134),  # BHF Angina (ICD10, SNOMED, READV2)
    ("PH986", 2164),  # BHF Unstable angina (ICD10, SNOMED, READV2)
    ("PH978", 2156),  # BHF Pulmonary embolism (ICD10, SNOMED, READV2)
    ("PH1016", 2194),  # BHF Peripheral arterial disease (SNOMED)
    ("PH1017", 2195),  # BHF Smoking status (SNOMED)
    ("PH942", 2120),  # BHF Acute myocardial infarction (ICD10, SNOMED)
    ("PH1005", 2183),  # BHF Depression (ICD10, SNOMED)
    ("PH981", 2159),  # BHF Pregnancy and birth (SNOMED)
    ("PH1004", 2182),  # BHF Dementia (ICD10, SNOMED)
    ("PH1006", 2184),  # BHF Diabetes (SNOMED)
    ("PH990", 2168),  # BHF Chronic kidney disease (ICD10, SNOMED)
    ("PH993", 2171),  # BHF Heart failure (ICD10, SNOMED)
    ("PH945", 2123),  # BHF Diabetes (ICD10, SNOMED)
    ("PH989", 2167),  # BHF Obesity (SNOMED)
    ("PH1010", 2188),  # BHF Hypertension (SNOMED)
    ("PH987", 2165),  # BHF Atrial fibrillation (ICD10, SNOMED)
    ("PH991", 2169),  # BHF Chronic obstructive pulmonary disease (ICD10, SNOMED)
    ("PH960", 2138),  # BHF Cancer (ICD10, SNOMED, READV2)
    ("PH1009", 2187),  # BHF Hypercholesterolaemia (SNOMED)
    ("PH1001", 2179),  # BHF Cancer (ICD10, SNOMED)
]
######################################################


def retrieve_hdruk_definition(phenotype_id: str, version_id: int) -> list[dict]:
    """
    Retrieves definition data from HDRUK API and returns as a List of dictionaries.
    Args:
        phenotype_id (str):
            ID of the definition
        version_id (int):
            Version of the definition
    Returns:
        List containing definition data (dict per code item)
    """
    print(f"Processing HDRUK definition {phenotype_id} version {version_id}...")

    hdr_client = HDRUKLibraryClient()

    # Get codelist data from API
    codelist_df = hdr_client.get_phenotype_codelist(
        phenotype_id=phenotype_id, version_id=version_id, output_format="db"
    )

    codelist_df["definition_id"] = phenotype_id
    codelist_df["definition_source"] = "HDRUK"
    codelist_df["definition_name"] = codelist_df["phenotype_name"]
    codelist_df["definition_version"] = codelist_df["phenotype_version"].astype(str)
    # print(codelist_df.columns)

    # Transform to definition object
    definition = Definition.from_dataframe(codelist_df)
    definition.uploaded_datetime = datetime.now()

    return definition.aslist

def retrieve_hdruk_definitions_from_list(definition_list: list) -> pd.DataFrame:
    """
    Takes a list of tuples representing HDRUK defintions and returns a dataframe with them all represented.
    Args:
        definition_list (list):
            List of tuples of phenotype ID, version ID
    Returns:
        DataFrame containing all data
    """

    all_definitions = []
    for phenotype_id, version_id in definition_list:
        new_def = retrieve_hdruk_definition(phenotype_id, version_id)
        all_definitions.extend(new_def)
    # print(all_definitions)

    print('HDRUK definitions retrieved successfully')
    df = pd.DataFrame(all_definitions)
    df.columns = df.columns.str.upper()  # Ensure all columns are uppercase

    # print(df)
    # print(df.dtypes)

    return df

if __name__ == "__main__":
    df = retrieve_hdruk_definitions_from_list(definition_list)
    path = "data/hdruk/hdruk_definitions.parquet"
    df.to_parquet(path, index=False)
    print(f"HDRUK definitions saved to {path}")