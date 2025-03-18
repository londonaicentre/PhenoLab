import pandas as pd
from dotenv import load_dotenv

from phmlondon.snow_utils import SnowflakeConnection

# Generates INTELLIGENCE_DEV.AI_CENTRE_FEATURE_STORE.COHORT_TABLE

CREATE_COHORT_TABLE = """
CREATE OR REPLACE TABLE INTELLIGENCE_DEV.AI_CENTRE_FEATURE_STORE.COHORT_TABLE AS
select 
pt.person_id,
pt.sk_patientid,
pt.date_of_birth,
gender,
ethnic_aic_category,
admission_date,
admission_time,
discharge_date,
discharge_time,
admission_type,
admission_sub_type,
spell_discharge_length_of_hospital_stay,
primary_diagnosis_code,
procedure_code,
episode_main_specialty,
los_unadjusted_days,
patient_type,
patient_age_at_activity
from PROD_DWH.ANALYST_PRIMARY_CARE.PATIENT pt
left join INTELLIGENCE_DEV.AI_CENTRE_FEATURE_STORE.PERSON_NEL_MASTER_INDEX nel_index on pt.person_id = nel_index.person_id
left join PROD_DWH.ANALYST_FACTS_UNIFIED_SUS.ADMITTED_PATIENT_CARE_SUMMARY sus_pc on pt.sk_patientid = sus_pc.sk_patientid
where nel_index.person_id is not null and sus_pc.sk_patientid is not null
order by person_id, admission_date
"""

def main():
    load_dotenv()


    try:
        snowsesh = SnowflakeConnection()
        snowsesh.use_database("INTELLIGENCE_DEV")
        snowsesh.use_schema("AI_CENTRE_FEATURE_STORE")

        snowsesh.execute_query(CREATE_COHORT_TABLE)

        print("Master person index table updated")

    except Exception as e:
        print(f"Error creating feature store: {e}")
        raise e
    finally:
        snowsesh.session.close()

if __name__ == "__main__":
    main()