## prevents load from failing
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

## Must be run from update.py
from dotenv import load_dotenv
from phmlondon.snow_utils import SnowflakeConnection
from phmlondon.hdruk_api import HDRUKLibraryClient
from loaders.base.phenotype import Phenotype
from datetime import datetime
import pandas as pd
from loaders.base.load_tables import load_phenotypes_to_snowflake

#######################################################
# Active phenotypes for retrieval from HDR-UK library #
phenotype_list = [
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
    ("PH1001", 2179), # BHF Cancer (ICD10, SNOMED)
]
#######################################################

def retrieve_hdruk_phenotypes(
        phenotype_id: str,
        version_id: int
        ) -> pd.DataFrame:
    """
    Retrieves phenotype data from HDRUK API and returns as a DataFrame.
    Args:
        phenotype_id (str):
            ID of the phenotype
        version_id (int):
            Version of the phenotype
    Returns:
        DataFrame containing phenotype data
    """
    try:
        print(f"Processing phenotype {phenotype_id} version {version_id}...")

        hdr_client = HDRUKLibraryClient()

        # Get codelist data from API
        codelist_df = hdr_client.get_phenotype_codelist(
            phenotype_id=phenotype_id,
            version_id=version_id,
            output_format="db"
        )

        # Transform to phenotype object
        phenotype = Phenotype.from_dataframe(codelist_df)
        phenotype.uploaded_datetime = datetime.now()

        # Convert to pandas df
        df = phenotype.to_dataframe()
        df.columns = df.columns.str.upper()

        print(f"Retrieved and transformed phenotype {phenotype_id}")
        print("DataFrame preview:")
        print(df.head())

        return df

    except Exception as e:
        print(f"Error retrieving phenotype {phenotype_id}: {e}")
        raise e

def main():
    load_dotenv()

    snowsesh = SnowflakeConnection()
    snowsesh.use_database("INTELLIGENCE_DEV")
    snowsesh.use_schema("AI_CENTRE_PHENOTYPE_LIBRARY")

    try:
        for phenotype_id, version_id in phenotype_list:
            df = retrieve_hdruk_phenotypes(phenotype_id, version_id)

            load_phenotypes_to_snowflake(
                snowsesh=snowsesh,
                df=df,
                table_name="HDRUK_PHENOTYPES"
            )
            print(f"Completed processing phenotype {phenotype_id}")

    except Exception as e:
        print(f"Failed to load phenotype {phenotype_id}: {e}")
        raise e
    finally:
        snowsesh.session.close()

if __name__ == "__main__":
    print(f"ERROR: This script should not be run directly.")
    print("Please run from update.py using the appropriate flag.")
    sys.exit(1)