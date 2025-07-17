# Generates BASE_UNIFIED_EMERGENCY_CARE
# Contains all primary and secondary diagnoses, procedures and investigations
# from emergency department encounters
# Transforms all codes to standard form

# List all icd_code_columns:
icd_columns = {'emergency_care_primary_diagnosis_icd' : 'emergency_care_primary_diagnosis_icd_code',
               'emergency_care_secondary_diagnosis_icd' : 'emergency_care_secondary_diagnosis_icd_code'}
for i in range(3, 16):
    icd_columns['diagnosis_icd_%02d' % i] = 'diagnosis_icd_code_%02d' % i

snomed_columns = {'emergency_care_primary_diagnosis' : 'emergency_care_primary_diagnosis_snomed',
               'emergency_care_secondary_diagnosis' : 'emergency_care_secondary_diagnosis_snomed'}
for i in range(3, 16):
    snomed_columns['diagnosis_snomed_%02d' % i] = 'diagnosis_snomed_code_%02d' % i

BASE_UNIFIED_EMERGENCY_CARE_SQL = f"""
WITH patient_mapping AS (
    SELECT DISTINCT
        SK_PATIENTID,
        PERSON_ID
    FROM PROD_DWH.ANALYST_FACTS.PMI
),
-- Extract all diagnoses
diagnosis_concepts AS ({' '.join(
    '''
    SELECT
        s.ATTENDANCE_IDENTIFIER AS ATTENDANCE_ID,
        p.PERSON_ID,
        s.SK_PATIENTID,
        s.EMERGENCY_CARE_ARRIVAL_DATE AS ACTIVITY_DATE,
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
    '''.format(code=code, icd=icd) for icd, code in icd_columns.items())}

    {' '.join(
    '''
    SELECT
        s.ATTENDANCE_IDENTIFIER AS ATTENDANCE_ID,
        p.PERSON_ID,
        s.SK_PATIENTID,
        s.EMERGENCY_CARE_ARRIVAL_DATE AS ACTIVITY_DATE,
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
        s.{snomed_code} AS CONCEPT_CODE,
        s.{snomed} AS CONCEPT_NAME,
        'DIAGNOSIS' AS CONCEPT_TYPE,
        'SNOMED' AS VOCABULARY,
        --s.SOURCE
    FROM PROD_DWH.ANALYST_FACTS_UNIFIED_SUS.UNIFIED_SUS_EMERGENCY_CARE s
    INNER JOIN patient_mapping p ON s.SK_PATIENTID = p.SK_PATIENTID
    WHERE s.{snomed_code} IS NOT NULL

    UNION ALL
    '''.format(snomed=snomed, snomed_code=snomed_columns[snomed]) for snomed in list(snomed_columns.keys())[:-1]
    )}
    SELECT
        s.ATTENDANCE_IDENTIFIER AS ATTENDANCE_ID,
        p.PERSON_ID,
        s.SK_PATIENTID,
        s.EMERGENCY_CARE_ARRIVAL_DATE AS ACTIVITY_DATE,
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
        WHEN VOCABULARY = 'ICD10'
        THEN
            LEFT(CONCEPT_CODE_CLEAN, 3) || '.' || SUBSTR(CONCEPT_CODE_CLEAN, 4)
        ELSE
            CONCEPT_CODE_CLEAN
    END AS CONCEPT_CODE_STD,
    CONCEPT_NAME,
    CONCEPT_TYPE,
    VOCABULARY
FROM cleaned_concepts
"""


def main():
    """
    Creates the BASE_UNIFIED_EMERGENCY_CARE table for NEL ICB warehouse in both prod and dev schemas.
    """
    from snowflake.snowpark import Session

    DATABASE = "INTELLIGENCE_DEV"
    SCHEMAS = ["AI_CENTRE_FEATURE_STORE", "PHENOLAB_DEV"]  # prod and dev
    CONNECTION_NAME = "nel_icb"

    try:
        session = Session.builder.config("connection_name", CONNECTION_NAME).create()

        for schema in SCHEMAS:
            print(f"Creating BASE_UNIFIED_EMERGENCY_CARE table in {DATABASE}.{schema}...")

            session.sql(f"""
                CREATE OR REPLACE TABLE {DATABASE}.{schema}.BASE_UNIFIED_EMERGENCY_CARE AS
                {BASE_UNIFIED_EMERGENCY_CARE_SQL}
            """).collect()

            print(f"BASE_UNIFIED_EMERGENCY_CARE table created successfully in {schema}.")

        print("All BASE_UNIFIED_EMERGENCY_CARE tables created successfully.")

    except Exception as e:
        print(f"Error creating BASE_UNIFIED_EMERGENCY_CARE table: {e}")
        raise e
    finally:
        session.close()


if __name__ == "__main__":
    main()