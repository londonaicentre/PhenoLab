WITH bp_data AS (
    SELECT
        obs.id,
        obs.patient_id,
        obs.person_id,
        obs.age_at_event,
        obs.clinical_effective_date, 
        obs.result_value, 
        obs.result_value_units, 
        pheno.code_description,
        'any reading' AS source -- Flag to identify observation data
    FROM PROD_DWH.ANALYST_PRIMARY_CARE.OBSERVATION AS obs
    JOIN PROD_DWH.ANALYST_PRIMARY_CARE.CONCEPT AS concept
        ON obs.core_concept_id = concept.dbid
    JOIN INTELLIGENCE_DEV.AI_CENTRE_PHENOTYPE_LIBRARY.PHENOSTORE AS pheno
        ON concept.CODE = pheno.code
    WHERE pheno.PHENOTYPE_ID = '999012731000230108'
    AND obs.result_value IS NOT NULL
    UNION ALL
    SELECT
        obs.id,
        obs.patient_id,
        obs.person_id,
        obs.age_at_event,
        obs.clinical_effective_date, 
        obs.result_value, 
        obs.result_value_units, 
        pheno.code_description,
        'ambulatory' AS source
    FROM PROD_DWH.ANALYST_PRIMARY_CARE.OBSERVATION AS obs
    JOIN PROD_DWH.ANALYST_PRIMARY_CARE.CONCEPT AS concept
        ON obs.core_concept_id = concept.dbid
    JOIN INTELLIGENCE_DEV.AI_CENTRE_PHENOTYPE_LIBRARY.PHENOSTORE AS pheno
        ON concept.CODE = pheno.code
    WHERE pheno.PHENOTYPE_ID = '999036291000230105'
    AND obs.result_value IS NOT NULL
),
full_bp AS (
    SELECT
        a.patient_id,
        a.age_at_event,
        a.clinical_effective_date,
        CASE 
            WHEN a.code_description ILIKE '%systolic%' THEN a.result_value
            WHEN b.code_description ILIKE '%systolic%' THEN b.result_value
            ELSE NULL
        END AS systolic_value,
        CASE 
            WHEN a.code_description ILIKE '%diastolic%' THEN a.result_value
            WHEN b.code_description ILIKE '%diastolic%' THEN b.result_value
            ELSE NULL
        END AS diastolic_value,
        a.code_description AS code_1,
        b.code_description AS code_2,
        a.source AS source_1,
        b.source AS source_2,
        CASE WHEN b.id IS NULL THEN 'Unmatched' ELSE 'Matched' END AS match_status
    FROM bp_data AS a
    LEFT JOIN bp_data AS b
        ON a.clinical_effective_date = b.clinical_effective_date
        AND a.patient_id = b.patient_id
        AND a.id <> b.id
    WHERE a.clinical_effective_date >= DATEADD(MONTH, -1, CURRENT_DATE)
    AND (
        (a.code_description ILIKE '%systolic%' AND b.code_description ILIKE '%diastolic%')
        OR 
        (a.code_description ILIKE '%diastolic%' AND b.code_description ILIKE '%systolic%')
    )
),
hypertension_labels AS (
    SELECT 
        full_bp.*,
        FALSE AS diagnosis_from_code,
        CASE 
            WHEN full_bp.source_1 = 'ambulatory'
            -- I initially had it that both readings had to be 'ambulatory' but found a bunch where they were coded as 'self reported systolic blood pressure' (ambulatory) and 'Diastolic arterial pressure' (not ambulatory) -> decided not to enforce matching
            THEN
                CASE
                    WHEN systolic_value >= 150 OR diastolic_value >= 95 
                    THEN 'Stage 2 Hypertension'
                    WHEN systolic_value >= 135 OR diastolic_value >= 85
                    THEN 'Stage 1 Hypertension'
                    ELSE 'Normal'
                END
            WHEN full_bp.source_1 = 'any reading'
            THEN 
                CASE
                    WHEN systolic_value >= 180 OR diastolic_value >= 120 
                    THEN 'Very high reading'
                    WHEN systolic_value >= 140 OR diastolic_value >= 90
                    THEN 'High reading'
                    ELSE 'Normal'
                END
            END AS hypertension_label
        FROM full_bp), 
diagnosed_by_code AS
(
    SELECT
        obs.patient_id,
        obs.clinical_effective_date, 
        obs.age_at_event,
        pheno.code_description, 
        TRUE AS diagnosis_from_code,
        'Coded' AS hypertension_label,
        NULL AS systolic_value,
        NULL AS diastolic_value,
    FROM PROD_DWH.ANALYST_PRIMARY_CARE.OBSERVATION AS obs
    JOIN PROD_DWH.ANALYST_PRIMARY_CARE.CONCEPT AS concept
        ON obs.core_concept_id = concept.dbid
    JOIN INTELLIGENCE_DEV.AI_CENTRE_PHENOTYPE_LIBRARY.PHENOSTORE AS pheno
        ON concept.CODE = pheno.code
    WHERE pheno.PHENOTYPE_ID = '999012971000230108' --- this is hypertension monitoring!! Need a proper hypertension codelist!!!!!!!!!!!!!!!!!!
), 
count_repeated_readings AS (
    SELECT 
        a.patient_id,
        a.age_at_event,
        a.clinical_effective_date,
        a.diagnosis_from_code,
        a.hypertension_label,
    (SELECT COUNT(*)
     FROM hypertension_labels b
     WHERE a.patient_id = b.patient_id
     AND b.clinical_effective_date BETWEEN DATEADD(YEAR, -1, a.clinical_effective_date) 
                                       AND a.clinical_effective_date
     AND b.hypertension_label = 'High reading'
    ) AS count_high_readings_in_year_prior,
    (SELECT COUNT(*)
     FROM hypertension_labels b
     WHERE a.patient_id = b.patient_id
     AND b.clinical_effective_date BETWEEN DATEADD(YEAR, -1, a.clinical_effective_date) 
                                       AND a.clinical_effective_date
     AND b.hypertension_label = 'Very high reading'
    ) AS count_very_high_readings_in_year_prior
FROM hypertension_labels a),
diagnosis_flag_from_readings AS (
SELECT 
    patient_id,
    age_at_event,
    clinical_effective_date,
    diagnosis_from_code,
    CASE
        WHEN count_very_high_readings_in_year_prior >= 3
        THEN 2
        WHEN count_high_readings_in_year_prior >=3
        THEN 1
        ELSE 0
    END AS diagnosis_from_repeated_readings
FROM 
    count_repeated_readings), 
combined_table AS (
SELECT
    patient_id,
    age_at_event,
    clinical_effective_date,
    diagnosis_from_code,
    diagnosis_from_repeated_readings as htn_stage
FROM diagnosis_flag_from_readings
UNION ALL
SELECT
    patient_id,
    age_at_event,
    clinical_effective_date,
    diagnosis_from_code,
    CASE
        WHEN hypertension_label = 'Stage 2 Hypertension'
        THEN 2
        WHEN hypertension_label = 'Stage 1 Hypertension'
        THEN 1
        ELSE 0
    END AS htn_stage
FROM hypertension_labels
UNION ALL
SELECT
    patient_id,
    age_at_event,
    clinical_effective_date,
    diagnosis_from_code,
    NULL AS htn_stage
FROM diagnosed_by_code)
    SELECT *
    FROM combined_table
    ORDER BY patient_id, age_at_event;
-- SELECT
--     patient_id,
--     first_value(age_at_event) over (partition by patient_id order by clinical_effective_date) as age_at_diagnosis,
--     first_value(clinical_effective_date) over (partition by patient_id order by clinical_effective_date) as date_of_diagnosis,
--     first_value(diagnosis_from_code) over (partition by patient_id order by clinical_effective_date) as diagnosis_from_clinical_code,
--     first_value(htn_stage) over (partition by patient_id order by clinical_effective_date) as htn_stage_if_diagnosed_from_readings
-- FROM combined_table
-- WHERE htn_stage = 2 
--     OR htn_stage = 1
--     OR htn_stage = NULL;