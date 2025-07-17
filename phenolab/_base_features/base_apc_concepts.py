
# Generates BASE_SUS_APC_CONCEPTS
# Contains all primary and secondary diagnoses, procedures and investigations
# from inpatient hospital activity (Admitted Patient Care)
# Transforms all codes to standard form
#
# TO DO
# Unit tests for code cleaning regex

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
),
-- Combine all concept types into one result set
all_concepts AS (
    SELECT * FROM diagnosis_concepts
    UNION ALL
    SELECT * FROM procedure_concepts
    UNION ALL
    SELECT * FROM investigation_concepts
),
-- standardise ICD10/OPCS4 type coding
split_concepts AS (
    SELECT
        *,
        CASE
            WHEN CONCEPT_CODE IS NULL THEN NULL
            ELSE
                -- Split by + and keep only the first part
                SPLIT_PART(CONCEPT_CODE, '+', 1)
        END AS CONCEPT_CODE_SPLIT
    FROM all_concepts
),
cleaned_concepts AS (
    SELECT
        *,
        CASE
            WHEN CONCEPT_CODE_SPLIT IS NULL THEN NULL
            ELSE
                -- Keep first character (may be X)
                LEFT(CONCEPT_CODE_SPLIT, 1) ||
                -- Remove other Xs and strip non alphanumeric
                REGEXP_REPLACE(
                    REGEXP_REPLACE(
                        SUBSTR(CONCEPT_CODE_SPLIT, 2),
                        'X', ''
                    ),
                    '[^A-Z0-9]', ''
                )
        END AS CONCEPT_CODE_CLEAN
    FROM split_concepts
)
SELECT
    ATTENDANCE_ID,
    PERSON_ID,
    SK_PATIENTID,
    ACTIVITY_DATE,
    LENGTH_OF_STAY,
    PROVIDER_NAME,
    TREATMENT_SITE_NAME,
    PATIENT_AGE,
    CONCEPT_CODE,
    -- Add '.' if above 3 chars
    CASE
        WHEN CONCEPT_CODE_CLEAN IS NULL THEN NULL
        WHEN LENGTH(CONCEPT_CODE_CLEAN) <= 3 THEN CONCEPT_CODE_CLEAN
        ELSE
            LEFT(CONCEPT_CODE_CLEAN, 3) || '.' || SUBSTR(CONCEPT_CODE_CLEAN, 4)
    END AS CONCEPT_CODE_STD,
    CONCEPT_NAME,
    CONCEPT_TYPE,
    VOCABULARY,
    ADMISSION_METHOD,
    DISCHARGE_DESTINATION,
    MAIN_SPECIALTY,
    SOURCE
FROM cleaned_concepts
"""


def main():
    """
    Creates the BASE_APC_CONCEPTS table for NEL ICB warehouse in both prod and dev schemas.
    """
    from snowflake.snowpark import Session

    DATABASE = "INTELLIGENCE_DEV"
    SCHEMAS = ["AI_CENTRE_FEATURE_STORE", "PHENOLAB_DEV"]  # prod and dev
    CONNECTION_NAME = "nel_icb"

    try:
        session = Session.builder.config("connection_name", CONNECTION_NAME).create()

        for schema in SCHEMAS:
            print(f"Creating BASE_APC_CONCEPTS table in {DATABASE}.{schema}...")

            session.sql(f"""
                CREATE OR REPLACE TABLE {DATABASE}.{schema}.BASE_APC_CONCEPTS AS
                {BASE_SUS_APC_CONCEPTS_SQL}
            """).collect()

            print(f"BASE_APC_CONCEPTS table created successfully in {schema}.")

        print("All BASE_APC_CONCEPTS tables created successfully.")

    except Exception as e:
        print(f"Error creating BASE_APC_CONCEPTS table: {e}")
        raise e
    finally:
        session.close()


if __name__ == "__main__":
    main()
