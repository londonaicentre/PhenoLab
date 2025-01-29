"""
Run this script to create the data view the dashboard is based on
"""

from phmlondon.snow_utils import SnowflakeConnection
from dotenv import load_dotenv

load_dotenv()
conn = SnowflakeConnection()
conn.use_database("INTELLIGENCE_DEV")
conn.use_schema("AI_CENTRE_DEV")

conn.session.sql(
"""
CREATE OR REPLACE VIEW patients_with_t2dm AS
    WITH code_list AS (
    SELECT code
    FROM INTELLIGENCE_DEV.AI_CENTRE_PHENOTYPE_LIBRARY.PHENOSTORE
    WHERE phenotype_source = 'LONDON' 
    AND Phenotype_id = '999010771000230109'
), 
mapped_ids AS (
    SELECT con.DBID
    FROM PROD_DWH.ANALYST_PRIMARY_CARE.CONCEPT AS con
    JOIN code_list cl 
    ON con.code = cl.code
), 
patient_list AS (
    SELECT obs.patient_id, obs.person_id
    FROM PROD_DWH.ANALYST_PRIMARY_CARE.OBSERVATION AS obs
    JOIN mapped_ids mi
    ON obs.core_concept_id = mi.DBID
    GROUP BY obs.patient_id, obs.person_id)
SELECT 
    patient_info.person_ID, 
    patient_info.gender_concept_ID,
    con.code AS gender_as_text,
    patient_info.DATE_OF_BIRTH,
    patient_info.DATE_OF_DEATH, 
    patient_info.CURRENT_ADDRESS_ID, 
    patient_info.ETHNIC_CODE_CONCEPT_ID,
    ethnicity.code AS ethnicity_as_text_code,
    patient_info.registered_practice_organization_id, 
    CASE 
        WHEN patient_info.date_of_death IS NOT NULL THEN patient_info.approx_current_age
        ELSE NULL 
    END AS age_if_patient_living
FROM PROD_DWH.ANALYST_PRIMARY_CARE.PATIENT AS patient_info
JOIN patient_list
ON patient_info.person_ID = patient_list.person_id
JOIN PROD_DWH.ANALYST_PRIMARY_CARE.CONCEPT AS con
ON patient_info.gender_concept_id = con.DBID
JOIN PROD_DWH.ANALYST_PRIMARY_CARE.CONCEPT AS ethnicity
ON patient_info.ethnic_code_concept_id = ethnicity.DBID;
"""
).collect()