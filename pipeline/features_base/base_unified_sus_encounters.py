import os
from dotenv import load_dotenv

from phmlondon.snow_utils import SnowflakeConnection
from phmlondon.feature_store_manager import FeatureStoreManager

from phmlondon.config import SNOWFLAKE_DATABASE, FEATURE_STORE, FEATURE_METADATA

# Generates BASE_SUS_ENCOUNTERS
# Contains core information about hospital encounters (inpatient, outpatient, emergency)
# without the detailed clinical concepts

BASE_SUS_ENCOUNTERS_SQL = """
WITH patient_mapping AS (
    SELECT DISTINCT
        SK_PATIENTID,
        PERSON_ID
    FROM PROD_DWH.ANALYST_FACTS.PMI
)
SELECT
    s.ATTENDANCE_IDENTIFIER AS ENCOUNTER_ID,
    s.SOURCE as ENCOUNTER_TYPE,
    p.PERSON_ID,
    s.SK_PATIENTID,
    s.ACTIVITY_DATE,
    s.LENGTH_OF_STAY,
    s.PROVIDER_CODE,
    s.PROVIDER_NAME,
    s.TREATMENT_SITE_CODE,
    s.TREATMENT_SITE_NAME,
    s.PATIENT_AGE,
    s.TREATMENT_FUNCTION_CODE,
    s.TREATMENT_FUNCTION_DESCRIPTION,
    s.ADMISSION_METHOD,
    s.ADMISSION_CATEGORY,
    s.ARRIVAL_MODE,
    s.DISCHARGE_DESTINATION,
    s.MAIN_SPECIALTY,
    s.HRG_CODE,
    s.HRG_NAME,
    s.TOTAL_ACTIVITY_COST
FROM PROD_DWH.ANALYST_FACTS_UNIFIED_SUS.SUS_AND_UNIFIED_SUS_COMPLETE_DATASET s
INNER JOIN patient_mapping p ON s.SK_PATIENTID = p.SK_PATIENTID
"""

def create_base_sus_encounters_feature(snowsesh, fsm):
    fsm.add_new_feature(
        feature_name="Base unified SUS Encounters",
        feature_desc="""
            Base feature table containing hospital encounter information.
            Includes all hospital visits (inpatient, outpatient, emergency) from 2015 onwards
            with key administrative, demographic, and financial details.
        """,
        feature_format="Wide, Mixed",
        sql_select_query_to_generate_feature=BASE_SUS_ENCOUNTERS_SQL,
        existence_ok=True,
    )

def main():
    load_dotenv()

    try:
        snowsesh = SnowflakeConnection()

        snowsesh.use_database(SNOWFLAKE_DATABASE)
        snowsesh.use_schema(FEATURE_STORE)

        fsm = FeatureStoreManager(
            connection=snowsesh,
            database=SNOWFLAKE_DATABASE,
            schema=FEATURE_STORE,
            metadata_schema=FEATURE_METADATA
        )
        print("Feature store manager created")

        create_base_sus_encounters_feature(snowsesh, fsm)
        print("Base SUS encounters table created")

    except Exception as e:
        print(f"Error creating feature store: {e}")
        raise e
    finally:
        snowsesh.session.close()


if __name__ == "__main__":
    main()