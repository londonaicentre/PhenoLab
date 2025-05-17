import os
from dotenv import load_dotenv

from phmlondon.snow_utils import SnowflakeConnection
from phmlondon.feature_store_manager import FeatureStoreManager

from phmlondon.config import SNOWFLAKE_DATABASE, FEATURE_STORE, FEATURE_METADATA

# Generates BASE_SUS_APC_CONCEPTS
# Contains all primary and secondary diagnoses, procedures and investigations
# from inpatient hospital activity (Admitted Patient Care)

BASE_SUS_APC_CONCEPTS_SQL = """
WITH patient_mapping AS (
    SELECT DISTINCT
        SK_PATIENTID,
        PERSON_ID
    FROM PROD_DWH.ANALYST_FACTS.PMI
),
-- Extract all diagnoses
diagnosis_concepts AS (
    SELECT
        s.ATTENDANCE_IDENTIFIER AS ATTENDANCE_ID,
        p.PERSON_ID,
        s.SK_PATIENTID,
        s.ACTIVITY_DATE,
        s.LENGTH_OF_STAY,
        s.PROVIDER_NAME,
        s.TREATMENT_SITE_NAME,
        s.PATIENT_AGE,
        s.PRIMARY_DIAGNOSIS_CODE AS CONCEPT_CODE,
        s.PRIMARY_DIAGNOSIS AS CONCEPT_NAME,
        'DIAGNOSIS' AS CONCEPT_TYPE,
        'ICD10' AS VOCABULARY,
        s.ADMISSION_METHOD,
        s.DISCHARGE_DESTINATION,
        s.MAIN_SPECIALTY,
        s.SOURCE
    FROM PROD_DWH.ANALYST_FACTS_UNIFIED_SUS.SUS_AND_UNIFIED_SUS_COMPLETE_DATASET s
    INNER JOIN patient_mapping p ON s.SK_PATIENTID = p.SK_PATIENTID
    WHERE s.PRIMARY_DIAGNOSIS_CODE IS NOT NULL
    AND s.SOURCE = 'Inpatient'
    AND s.ACTIVITY_DATE >= '2015-01-01'

    UNION ALL

    SELECT
        s.ATTENDANCE_IDENTIFIER AS ATTENDANCE_ID,
        p.PERSON_ID,
        s.SK_PATIENTID,
        s.ACTIVITY_DATE,
        s.LENGTH_OF_STAY,
        s.PROVIDER_NAME,
        s.TREATMENT_SITE_NAME,
        s.PATIENT_AGE,
        s.SECONDARY_DIAGNOSIS_CODE AS CONCEPT_CODE,
        s.SECONDARY_DIAGNOSIS AS CONCEPT_NAME,
        'DIAGNOSIS' AS CONCEPT_TYPE,
        'ICD10' AS VOCABULARY,
        s.ADMISSION_METHOD,
        s.DISCHARGE_DESTINATION,
        s.MAIN_SPECIALTY,
        s.SOURCE
    FROM PROD_DWH.ANALYST_FACTS_UNIFIED_SUS.SUS_AND_UNIFIED_SUS_COMPLETE_DATASET s
    INNER JOIN patient_mapping p ON s.SK_PATIENTID = p.SK_PATIENTID
    WHERE s.SECONDARY_DIAGNOSIS_CODE IS NOT NULL
    AND s.SOURCE = 'Inpatient'
    AND s.ACTIVITY_DATE >= '2015-01-01'
),
-- Extract all procedures
procedure_concepts AS (
    SELECT
        s.ATTENDANCE_IDENTIFIER AS ATTENDANCE_ID,
        p.PERSON_ID,
        s.SK_PATIENTID,
        s.ACTIVITY_DATE,
        s.LENGTH_OF_STAY,
        s.PROVIDER_NAME,
        s.TREATMENT_SITE_NAME,
        s.PATIENT_AGE,
        s.PRIMARY_PROCEDURE_CODE AS CONCEPT_CODE,
        s.PRIMARY_PROCEDURE AS CONCEPT_NAME,
        'PROCEDURE' AS CONCEPT_TYPE,
        'OPCS4' AS VOCABULARY,
        s.ADMISSION_METHOD,
        s.DISCHARGE_DESTINATION,
        s.MAIN_SPECIALTY,
        s.SOURCE
    FROM PROD_DWH.ANALYST_FACTS_UNIFIED_SUS.SUS_AND_UNIFIED_SUS_COMPLETE_DATASET s
    INNER JOIN patient_mapping p ON s.SK_PATIENTID = p.SK_PATIENTID
    WHERE s.PRIMARY_PROCEDURE_CODE IS NOT NULL
    AND s.SOURCE = 'Inpatient'
    AND s.ACTIVITY_DATE >= '2015-01-01'

    UNION ALL

    SELECT
        s.ATTENDANCE_IDENTIFIER AS ATTENDANCE_ID,
        p.PERSON_ID,
        s.SK_PATIENTID,
        s.ACTIVITY_DATE,
        s.LENGTH_OF_STAY,
        s.PROVIDER_NAME,
        s.TREATMENT_SITE_NAME,
        s.PATIENT_AGE,
        s.SECONDARY_PROCEDURE_CODE AS CONCEPT_CODE,
        s.SECONDARY_PROCEDURE AS CONCEPT_NAME,
        'PROCEDURE' AS CONCEPT_TYPE,
        'OPCS4' AS VOCABULARY,
        s.ADMISSION_METHOD,
        s.DISCHARGE_DESTINATION,
        s.MAIN_SPECIALTY,
        s.SOURCE
    FROM PROD_DWH.ANALYST_FACTS_UNIFIED_SUS.SUS_AND_UNIFIED_SUS_COMPLETE_DATASET s
    INNER JOIN patient_mapping p ON s.SK_PATIENTID = p.SK_PATIENTID
    WHERE s.SECONDARY_PROCEDURE_CODE IS NOT NULL
    AND s.SOURCE = 'Inpatient'
    AND s.ACTIVITY_DATE >= '2015-01-01'
),
-- Extract all investigations
investigation_concepts AS (
    SELECT
        s.ATTENDANCE_IDENTIFIER AS ATTENDANCE_ID,
        p.PERSON_ID,
        s.SK_PATIENTID,
        s.ACTIVITY_DATE,
        s.LENGTH_OF_STAY,
        s.PROVIDER_NAME,
        s.TREATMENT_SITE_NAME,
        s.PATIENT_AGE,
        s.PRIMARY_INVESTIGATION_CODE AS CONCEPT_CODE,
        s.PRIMARY_INVESTIGATION AS CONCEPT_NAME,
        'INVESTIGATION' AS CONCEPT_TYPE,
        'OPCS4' AS VOCABULARY,
        s.ADMISSION_METHOD,
        s.DISCHARGE_DESTINATION,
        s.MAIN_SPECIALTY,
        s.SOURCE
    FROM PROD_DWH.ANALYST_FACTS_UNIFIED_SUS.SUS_AND_UNIFIED_SUS_COMPLETE_DATASET s
    INNER JOIN patient_mapping p ON s.SK_PATIENTID = p.SK_PATIENTID
    WHERE s.PRIMARY_INVESTIGATION_CODE IS NOT NULL
    AND s.SOURCE = 'Inpatient'
    AND s.ACTIVITY_DATE >= '2015-01-01'

    UNION ALL

    SELECT
        s.ATTENDANCE_IDENTIFIER AS ATTENDANCE_ID,
        p.PERSON_ID,
        s.SK_PATIENTID,
        s.ACTIVITY_DATE,
        s.LENGTH_OF_STAY,
        s.PROVIDER_NAME,
        s.TREATMENT_SITE_NAME,
        s.PATIENT_AGE,
        s.SECONDARY_INVESTIGATION_CODE AS CONCEPT_CODE,
        s.SECONDARY_INVESTIGATION AS CONCEPT_NAME,
        'INVESTIGATION' AS CONCEPT_TYPE,
        'OPCS4' AS VOCABULARY,
        s.ADMISSION_METHOD,
        s.DISCHARGE_DESTINATION,
        s.MAIN_SPECIALTY,
        s.SOURCE
    FROM PROD_DWH.ANALYST_FACTS_UNIFIED_SUS.SUS_AND_UNIFIED_SUS_COMPLETE_DATASET s
    INNER JOIN patient_mapping p ON s.SK_PATIENTID = p.SK_PATIENTID
    WHERE s.SECONDARY_INVESTIGATION_CODE IS NOT NULL
    AND s.SOURCE = 'Inpatient'
    AND s.ACTIVITY_DATE >= '2015-01-01'
)

-- Combine all concept types into one result set
SELECT * FROM diagnosis_concepts
UNION ALL
SELECT * FROM procedure_concepts
UNION ALL
SELECT * FROM investigation_concepts
"""

def create_base_sus_apc_concepts_feature(snowsesh, fsm):
    fsm.add_new_feature(
        feature_name="Base SUS_APC Concepts",
        feature_desc="""
            Key base feature table for downstream feature generation.
            Contains all primary and secondary diagnoses (ICD10), procedures and investigations (OPCS4)
            recorded during inpatient hospital stays (Admitted Patient Care) from 2015 onwards.
            Includes visit-level details such as provider, admission method, HRG code and cost.
        """,
        feature_format="Wide, Mixed",
        sql_select_query_to_generate_feature=BASE_SUS_APC_CONCEPTS_SQL,
        existence_ok=True,
    )

def main():
    """
    Main function to create the BASE_SUS_APC_CONCEPTS feature.
    """
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

        create_base_sus_apc_concepts_feature(snowsesh, fsm)
        print("base SUS APC concept table created")

    except Exception as e:
        print(f"Error creating feature store: {e}")
        raise e
    finally:
        snowsesh.session.close()


if __name__ == "__main__":
    main()