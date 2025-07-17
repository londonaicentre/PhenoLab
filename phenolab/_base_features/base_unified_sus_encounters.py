# Generates BASE_UNIFIED_SUS_ENCOUNTERS
# Contains core information about hospital encounters (inpatient, outpatient, emergency)
# without the detailed clinical concepts

BASE_UNIFIED_SUS_ENCOUNTERS_SQL = """
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

BASE_ENCOUNTER_TYPE_AE_ARRIVAL_SQL = """
SELECT
    ENCOUNTER_ID,
    PERSON_ID,
    ACTIVITY_DATE,
    ARRIVAL_MODE
FROM BASE_UNIFIED_SUS_ENCOUNTERS
WHERE ENCOUNTER_TYPE = 'AE'
"""

BASE_ENCOUNTER_TYPE_AE_HRG_SQL = """
SELECT
    ENCOUNTER_ID,
    PERSON_ID,
    ACTIVITY_DATE,
    HRG_NAME
FROM BASE_UNIFIED_SUS_ENCOUNTERS
WHERE ENCOUNTER_TYPE = 'AE'
"""

BASE_ENCOUNTER_TYPE_OPD_FUNCTION_SQL = """
SELECT
    ENCOUNTER_ID,
    PERSON_ID,
    ACTIVITY_DATE,
    TREATMENT_FUNCTION_DESCRIPTION
FROM BASE_UNIFIED_SUS_ENCOUNTERS
WHERE ENCOUNTER_TYPE = 'Outpatient'
"""

BASE_ENCOUNTER_TYPE_INPT_LOS_SQL = """
SELECT
    ENCOUNTER_ID,
    PERSON_ID,
    ACTIVITY_DATE,
    LENGTH_OF_STAY
FROM BASE_UNIFIED_SUS_ENCOUNTERS
WHERE ENCOUNTER_TYPE = 'Inpatient'
"""

BASE_ENCOUNTER_TYPE_INPT_METHOD_SQL = """
SELECT
    ENCOUNTER_ID,
    PERSON_ID,
    ACTIVITY_DATE,
    ADMISSION_METHOD
FROM BASE_UNIFIED_SUS_ENCOUNTERS
WHERE ENCOUNTER_TYPE = 'Inpatient'
"""


def main():
    """
    Creates the BASE_UNIFIED_SUS_ENCOUNTERS table for NEL ICB warehouse in both prod and dev schemas.
    """
    from snowflake.snowpark import Session

    DATABASE = "INTELLIGENCE_DEV"
    SCHEMAS = ["AI_CENTRE_FEATURE_STORE", "PHENOLAB_DEV"]  # prod and dev
    CONNECTION_NAME = "nel_icb"

    try:
        session = Session.builder.config("connection_name", CONNECTION_NAME).create()

        for schema in SCHEMAS:
            print(f"Creating BASE_UNIFIED_SUS_ENCOUNTERS table in {DATABASE}.{schema}...")

            session.sql(f"""
                CREATE OR REPLACE TABLE {DATABASE}.{schema}.BASE_UNIFIED_SUS_ENCOUNTERS AS
                {BASE_UNIFIED_SUS_ENCOUNTERS_SQL}
            """).collect()

            print(f"BASE_UNIFIED_SUS_ENCOUNTERS table created successfully in {schema}.")

            # Create views
            views = [
                ("BASE_ENCOUNTER_TYPE_AE_ARRIVAL", BASE_ENCOUNTER_TYPE_AE_ARRIVAL_SQL),
                ("BASE_ENCOUNTER_TYPE_AE_HRG", BASE_ENCOUNTER_TYPE_AE_HRG_SQL),
                ("BASE_ENCOUNTER_TYPE_OPD_FUNCTION", BASE_ENCOUNTER_TYPE_OPD_FUNCTION_SQL),
                ("BASE_ENCOUNTER_TYPE_INPT_LOS", BASE_ENCOUNTER_TYPE_INPT_LOS_SQL),
                ("BASE_ENCOUNTER_TYPE_INPT_METHOD", BASE_ENCOUNTER_TYPE_INPT_METHOD_SQL)
            ]

            for view_name, view_sql in views:
                session.sql(f"""
                    CREATE OR REPLACE VIEW {DATABASE}.{schema}.{view_name} AS
                    {view_sql}
                """).collect()
                print(f"{view_name} view created successfully in {schema}.")

        print("All BASE_UNIFIED_SUS_ENCOUNTERS tables and views created successfully.")

    except Exception as e:
        print(f"Error creating BASE_UNIFIED_SUS_ENCOUNTERS table: {e}")
        raise e
    finally:
        session.close()


if __name__ == "__main__":
    main()