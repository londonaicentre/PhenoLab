from dotenv import load_dotenv

from phmlondon.config import FEATURE_METADATA, FEATURE_STORE, SNOWFLAKE_DATABASE
from phmlondon.feature_store_manager import FeatureStoreManager
from phmlondon.snow_utils import SnowflakeConnection

# Generates BASE_SUS_EMERGENCY_CARE
# Contains all primary and secondary diagnoses, procedures and investigations
# from emergency department encounters
# Transforms all codes to standard form

#List all icd_code_columns:
icd_columns = {'emergency_care_primary_diagnosis_icd' : 'emergency_care_primary_diagnosis_icd_code',
               'emergency_care_secondary_diagnosis_icd' : 'emergency_care_secondary_diagnosis_icd_code'}
for i in range(3, 16):
    icd_columns['diagnosis_icd_%02d' % i] = 'diagnosis_icd_code_%02d' % i

snomed_columns = {'emergency_care_primary_diagnosis' : 'emergency_care_primary_diagnosis_snomed',
               'emergency_care_secondary_diagnosis' : 'emergency_care_secondary_diagnosis_snomed'}
for i in range(3, 16):
    snomed_columns['diagnosis_snomed_%02d' % i] = 'diagnosis_snomed_code_%02d' % i

BASE_SUS_EMERGENCY_CARE_SQL = f"""
WITH patient_mapping AS (
    SELECT DISTINCT
        SK_PATIENTID,
        PERSON_ID
    FROM PROD_DWH.ANALYST_FACTS.PMI
),
-- Extract all diagnoses
diagnosis_concepts AS ({' '.join(
    f"""
    SELECT
        s.ATTENDANCE_IDENTIFIER AS ATTENDANCE_ID,
        p.PERSON_ID,
        s.SK_PATIENTID,
        s.ACCIDENT_AND_EMERGENCY_INITIAL_ASSESSMENT_DATE AS ACTIVITY_DATE,
        s.arrival_mode,
        s.attendance_source,
        s.is_emergency_admisison AS admitted_to_hospital,
        s.activity_treatment_function_name_decision_to_admit as admitting_service,
        s.emergency_care_discharge_follow_up as follow_up,
        s.emergency_care_discharge_destination,
        s.emergency_care_discharge_status,
        s.conclusion_time_since_arrival as time_in_department_minutes,
        s.seen_for_treatment_time_since_arrival as time_to_treatment,
        s.department_type,
        s.organisation_site_name_identifier_of_treatment as treatment_site_name,
        s.attendance_location_hes_provider_name as provider_name,
        s.age_at_arrival as PATIENT_AGE,
        s.{code} AS CONCEPT_CODE,
        s.{icd} AS CONCEPT_NAME,
        'DIAGNOSIS' AS CONCEPT_TYPE,
        'ICD10' AS VOCABULARY,
        --s.SOURCE
    FROM PROD_DWH.ANALYST_FACTS_UNIFIED_SUS.UNIFIED_SUS_EMERGENCY_CARE s
    INNER JOIN patient_mapping p ON s.SK_PATIENTID = p.SK_PATIENTID
    WHERE s.{code} IS NOT NULL

    UNION ALL
    """ for icd, code in icd_columns.items())}

    {' '.join(
    f"""
    SELECT
        s.ATTENDANCE_IDENTIFIER AS ATTENDANCE_ID,
        p.PERSON_ID,
        s.SK_PATIENTID,
        s.ACCIDENT_AND_EMERGENCY_INITIAL_ASSESSMENT_DATE AS ACTIVITY_DATE,
        s.arrival_mode,
        s.attendance_source,
        s.is_emergency_admisison AS admitted_to_hospital,
        s.activity_treatment_function_name_decision_to_admit as admitting_service,
        s.emergency_care_discharge_follow_up as follow_up,
        s.emergency_care_discharge_destination,
        s.emergency_care_discharge_status,
        s.conclusion_time_since_arrival as time_in_department_minutes,
        s.seen_for_treatment_time_since_arrival as time_to_treatment,
        s.department_type,
        s.organisation_site_name_identifier_of_treatment as treatment_site_name,
        s.attendance_location_hes_provider_name as provider_name,
        s.age_at_arrival as PATIENT_AGE,
        s.{snomed_columns[snomed]} AS CONCEPT_CODE,
        s.{snomed} AS CONCEPT_NAME,
        'DIAGNOSIS' AS CONCEPT_TYPE,
        'SNOMED' AS VOCABULARY,
        --s.SOURCE
    FROM PROD_DWH.ANALYST_FACTS_UNIFIED_SUS.UNIFIED_SUS_EMERGENCY_CARE s
    INNER JOIN patient_mapping p ON s.SK_PATIENTID = p.SK_PATIENTID
    WHERE s.{snomed_columns[snomed]} IS NOT NULL

    UNION ALL
    """ for snomed in list(snomed_columns.keys())[:-1]
    )}
    SELECT
        s.ATTENDANCE_IDENTIFIER AS ATTENDANCE_ID,
        p.PERSON_ID,
        s.SK_PATIENTID,
        s.ACCIDENT_AND_EMERGENCY_INITIAL_ASSESSMENT_DATE AS ACTIVITY_DATE,
        s.arrival_mode,
        s.attendance_source,
        s.is_emergency_admisison AS admitted_to_hospital,
        s.activity_treatment_function_name_decision_to_admit as admitting_service,
        s.emergency_care_discharge_follow_up as follow_up,
        s.emergency_care_discharge_destination,
        s.emergency_care_discharge_status,
        s.conclusion_time_since_arrival as time_in_department_minutes,
        s.seen_for_treatment_time_since_arrival as time_to_treatment,
        s.department_type,
        s.organisation_site_name_identifier_of_treatment as treatment_site_name,
        s.attendance_location_hes_provider_name as provider_name,
        s.age_at_arrival as PATIENT_AGE,
        s.{list(snomed_columns.values())[-1]} AS CONCEPT_CODE,
        s.{list(snomed_columns.keys())[-1]} AS CONCEPT_NAME,
        'DIAGNOSIS' AS CONCEPT_TYPE,
        'SNOMED' AS VOCABULARY,
        --s.SOURCE
    FROM PROD_DWH.ANALYST_FACTS_UNIFIED_SUS.UNIFIED_SUS_EMERGENCY_CARE s
    INNER JOIN patient_mapping p ON s.SK_PATIENTID = p.SK_PATIENTID
    WHERE s.{list(snomed_columns.values())[-1]} IS NOT NULL
    ),

-- standardise ICD10/OPCS4 type coding
split_concepts AS (
    SELECT
        *,
        TRIM(value::STRING) AS CONCEPT_CODE_SPLIT
    FROM diagnosis_concepts,
    LATERAL FLATTEN(SPLIT(concept_code, ',')) AS f
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
    ARRIVAL_MODE,
    ATTENDANCE_SOURCE,
    ADMITTED_TO_HOSPITAL,
    ADMITTING_SERVICE,
    FOLLOW_UP,
    EMERGENCY_CARE_DISCHARGE_DESTINATION,
    EMERGENCY_CARE_DISCHARGE_STATUS,
    TIME_IN_DEPARTMENT_MINUTES,
    TIME_TO_TREATMENT,
    DEPARTMENT_TYPE,
    TREATMENT_SITE_NAME,
    PROVIDER_NAME,
    PATIENT_AGE,
    CONCEPT_CODE,
    -- Add '.' if above 3 chars
    CASE
        WHEN CONCEPT_CODE_CLEAN IS NULL THEN NULL
        WHEN CONCEPT_CODE_CLEAN LIKE '%#%' THEN NULL
        WHEN LENGTH(CONCEPT_CODE_CLEAN) <= 3 THEN CONCEPT_CODE_CLEAN
        ELSE
            LEFT(CONCEPT_CODE_CLEAN, 3) || '.' || SUBSTR(CONCEPT_CODE_CLEAN, 4)
    END AS CONCEPT_CODE_STD,
    CONCEPT_NAME,
    CONCEPT_TYPE,
    VOCABULARY
FROM cleaned_concepts
"""

def create_base_sus_emergency_care_feature(snowsesh, fsm):
    fsm.add_new_feature(
        feature_name="Base UNIFIED EMERGENCY CARE",
        feature_desc="""
            Key base feature table for downstream feature generation.
            Contains all primary and secondary diagnoses (ICD10 and SNOMED)
            recorded during emergency department visit from 2015 onwards.
            Includes visit-level details such as provider, location.
        """,
        feature_format="Wide, Mixed",
        sql_select_query_to_generate_feature=BASE_SUS_EMERGENCY_CARE_SQL,
        existence_ok=True,
    )

def main():
    """
    Main function to create the BASE_UNIFIED_EMERGENCY_CARE feature.
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

        create_base_sus_emergency_care_feature(snowsesh, fsm)
        print("base unified emergency care table created")

    except Exception as e:
        print(f"Error creating feature store: {e}")
        raise e
    finally:
        snowsesh.session.close()


if __name__ == "__main__":
    main()
