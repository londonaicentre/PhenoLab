"""
Run this script to create the data views the dashboard is based on
Note all data views will be rebuilt and overwrite the existing!
"""

from dotenv import load_dotenv

from phmlondon.snow_utils import SnowflakeConnection

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
    SELECT obs.patient_id, obs.person_id, min(date_recorded) AS diagnosis_date
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
    demographics.ethnicity_detail AS ethnicity_description,
    demographics.ethnicity_main_category  AS ethnic_category,
    patient_info.registered_practice_organization_id,
    CASE
        WHEN patient_info.date_of_death IS NOT NULL THEN patient_info.approx_current_age
        ELSE NULL
    END AS age_if_patient_living,
    patient_list.diagnosis_date
FROM PROD_DWH.ANALYST_PRIMARY_CARE.PATIENT AS patient_info
JOIN patient_list
ON patient_info.person_ID = patient_list.person_id
JOIN PROD_DWH.ANALYST_PRIMARY_CARE.CONCEPT AS con
ON patient_info.gender_concept_id = con.DBID
JOIN PROD_DWH.ANALYST_PRIMARY_CARE.CONCEPT AS ethnicity
ON patient_info.ethnic_code_concept_id = ethnicity.DBID
JOIN intelligence_dev.ai_centre_feature_store.person_nel_master_index AS demographics
ON patient_info.person_id = demographics.person_id;
"""
).collect()


conn.session.sql(
    """
CREATE OR REPLACE VIEW observations_with_t2dm AS
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
obervation_list AS (
    SELECT *
    FROM PROD_DWH.ANALYST_PRIMARY_CARE.OBSERVATION AS obs
    JOIN mapped_ids mi
    ON obs.core_concept_id = mi.DBID)
SELECT *
FROM obervation_list o;"""
).collect()

conn.session.sql(
    """
CREATE OR REPLACE VIEW hba1c_levels AS
    WITH code_list AS (
    SELECT code
    FROM INTELLIGENCE_DEV.AI_CENTRE_PHENOTYPE_LIBRARY.PHENOSTORE
    WHERE phenotype_source = 'LONDON'
    AND Phenotype_id = '999023291000230101'
),
mapped_ids AS (
    SELECT con.DBID
    FROM PROD_DWH.ANALYST_PRIMARY_CARE.CONCEPT AS con
    JOIN code_list cl
    ON con.code = cl.code
),
obs_list AS (
    SELECT *
    FROM PROD_DWH.ANALYST_PRIMARY_CARE.OBSERVATION AS obs
    JOIN mapped_ids mi
    ON obs.core_concept_id = mi.DBID)
SELECT * FROM obs_list;"""
).collect()

# from data exploration, it seems that results value doesn't match the units or each other
# e.g. having 63 mmol/L and 7.1 mmol/L!!! 7 mmol/mol and 81 mmol/mol
# so we need to allocate units by number!!! as the units seem to be a total mess!!

# https://www.ncbi.nlm.nih.gov/books/NBK348987/#:~:text=TABLE%2085&text=IFCC%2C%20International%20Federation%20of%20Clinical,)%20–%2023.5%20mmol/mol.

conn.session.sql(
    """
create or replace view hba1c_levels_with_corrected_units AS
select *,
    CASE
        WHEN result_value BETWEEN 4 AND 14 THEN '%'
        WHEN result_value BETWEEN 19 and 140 THEN 'mmol/mol'
        ELSE 'invalid'
    END AS result_value_inferred_units,
    CASE
        WHEN result_value BETWEEN 4 and 15 THEN ROUND((result_value*10.93) - 23.5, 2)
        WHEN result_value BETWEEN 19 and 140 THEN result_value
        ELSE NULL -- invalid values set to null
    END AS result_value_cleaned_and_converted_to_mmol_per_mol
from INTELLIGENCE_DEV.AI_CENTRE_DEV.HBA1C_LEVELS;"""
).collect()

conn.session.sql(
    """
CREATE OR REPLACE VIEW undiagnosed_patients_with_T2DM AS
WITH diabetic_hba1c_patients AS (
    SELECT person_id, MAX(result_value_cleaned_and_converted_to_mmol_per_mol) AS max_hba1c
    FROM INTELLIGENCE_DEV.AI_CENTRE_DEV.HBA1C_LEVELS_WITH_CORRECTED_UNITS
    WHERE result_value_cleaned_and_converted_to_mmol_per_mol >= 48
    GROUP BY person_id
)
SELECT h.person_id, h.max_hba1c
FROM diabetic_hba1c_patients h
LEFT JOIN INTELLIGENCE_DEV.AI_CENTRE_DEV.PATIENTS_WITH_T2DM p ON h.person_id = p.person_id
WHERE p.person_id IS NULL;"""
)
