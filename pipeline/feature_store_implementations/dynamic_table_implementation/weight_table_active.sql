-- code to be run in snowflake with a create or replace table intelligence_dev.ai_centre_dev.weight_table_active AS 
-- makes table with one row per active NEL patient who has BMI data
-- BMI data made up of 1. coded category (without value)
-- 2. BMI value
-- 3. calculated from height and weight values.
-- this is then used to categorise into healthy weight, underweight, overweight, obese class I,II,II or obese unclassified.
-- unknowns were due to childrens classifications and have been removed from this table


WITH coded_weight_class AS (
    -- Coded weight class diagnoses
    SELECT
        obs.id,
        obs.patient_id,
        obs.person_id,
        obs.age_at_event,
        obs.clinical_effective_date, 
        obs.result_value as value, 
        obs.result_value_units, 
        pheno.code_description,
        'coded' AS source
    FROM PROD_DWH.ANALYST_PRIMARY_CARE.OBSERVATION AS obs
    JOIN PROD_DWH.ANALYST_PRIMARY_CARE.CONCEPT AS concept
        ON obs.core_concept_id = concept.dbid
    JOIN INTELLIGENCE_DEV.AI_CENTRE_PHENOTYPE_LIBRARY.PHENOSTORE AS pheno
        ON concept.CODE = pheno.code
    WHERE pheno.PHENOTYPE_ID IN ('999016011000230101', '999016051000230102', 
    '999016131000230105', '999016091000230107', '999011051000230106', 
    '999020771000230102') -- NHS PCD Refsets for coded weight status (healthy, under, over, and obese classes)
AND (obs.result_value IS NULL OR obs.result_value BETWEEN 50 AND 150)

    UNION ALL

    -- Direct BMI readings
    SELECT
        obs.id,
        obs.patient_id,
        obs.person_id,
        obs.age_at_event,
        obs.clinical_effective_date, 
        obs.result_value as value, 
        obs.result_value_units, 
        pheno.code_description,
        'any reading' AS source
    FROM PROD_DWH.ANALYST_PRIMARY_CARE.OBSERVATION AS obs
    JOIN PROD_DWH.ANALYST_PRIMARY_CARE.CONCEPT AS concept
        ON obs.core_concept_id = concept.dbid
    JOIN INTELLIGENCE_DEV.AI_CENTRE_PHENOTYPE_LIBRARY.PHENOSTORE AS pheno
        ON concept.CODE = pheno.code
    WHERE pheno.PHENOTYPE_ID IN ('999011171000230101') -- BMI coded with associated value NHS PCD refset
    AND obs.result_value IS NOT NULL
    AND obs.result_value BETWEEN 50 AND 150 

    UNION ALL

    -- Calculated BMI using height and weight values from measururemnts within a year of eachother.
    SELECT
        w.id AS id,  
        w.patient_id,
        w.person_id,
        w.age_at_event,
        w.clinical_effective_date, -- Use weight date as the clinical date
        CASE 
            WHEN w.age_at_event >= 18  -- Apply validation only for adults
                 AND h.result_value_units = 'cm' 
                 AND w.result_value BETWEEN 30 AND 300
                 AND h.result_value BETWEEN 50 AND 250 
            THEN ROUND(w.result_value / POWER((h.result_value / 100), 2),2)  -- Convert cm to meters before squaring

            WHEN w.age_at_event >= 18  -- Apply validation only for adults
                 AND h.result_value_units = 'm'  
                 AND w.result_value BETWEEN 30 AND 300
                 AND h.result_value BETWEEN 0.5 AND 2.5
            THEN ROUND(w.result_value / POWER(h.result_value, 2),2)  -- Use meters directly

            ELSE NULL  
        END AS value,    -- BMI calculation as result_value, round to 2 dp
        'kg/m2' AS result_value_units,
        'Calculated BMI from height & weight' AS code_description,
        'calculated' AS source
    FROM PROD_DWH.ANALYST_PRIMARY_CARE.OBSERVATION AS w
    JOIN PROD_DWH.ANALYST_PRIMARY_CARE.OBSERVATION AS h
        ON w.patient_id = h.patient_id
        AND ABS(DATEDIFF(YEAR, w.clinical_effective_date, h.clinical_effective_date)) <= 1 -- up to one year difference in reading dates 
    JOIN PROD_DWH.ANALYST_PRIMARY_CARE.CONCEPT AS concept_w
        ON w.core_concept_id = concept_w.dbid
    JOIN PROD_DWH.ANALYST_PRIMARY_CARE.CONCEPT AS concept_h
        ON h.core_concept_id = concept_h.dbid
    JOIN INTELLIGENCE_DEV.AI_CENTRE_PHENOTYPE_LIBRARY.PHENOSTORE AS pheno_w
        ON concept_w.CODE = pheno_w.code
        AND pheno_w.PHENOTYPE_ID = 'opensafely/weight-snomed' -- Weight codes
    JOIN INTELLIGENCE_DEV.AI_CENTRE_PHENOTYPE_LIBRARY.PHENOSTORE AS pheno_h
        ON concept_h.CODE = pheno_h.code
        AND pheno_h.PHENOTYPE_ID = 'opensafely/height-snomed' -- Height codes
    WHERE value IS NOT NULL
),
categorised_data AS (
    -- Add the calculated weight class column
    SELECT *,
        CASE 
            -- Classification by value
            WHEN value < 18.5 THEN 'Underweight'
            WHEN value BETWEEN 18.5 AND 24.9 THEN 'Healthy weight'
            WHEN value BETWEEN 25 AND 29.9 THEN 'Overweight'
            WHEN value BETWEEN 30 AND 34.9 THEN 'Obese Class I'
            WHEN value BETWEEN 35 AND 39.9 THEN 'Obese Class II'
            WHEN value >= 40 THEN 'Obese Class III'

            -- Classification by code_description if value is NULL
            WHEN value IS NULL AND code_description ILIKE '%underweight%' THEN 'Underweight'
            WHEN value IS NULL AND code_description ILIKE '%less than 16.5%' THEN 'Underweight'
            WHEN value IS NULL AND code_description ILIKE '%normal%' THEN 'Healthy weight'
            WHEN value IS NULL AND code_description ILIKE '%healthy%' THEN 'Healthy weight'
            WHEN value IS NULL AND code_description ILIKE '%overweight%' THEN 'Overweight'
            WHEN value IS NULL AND code_description ILIKE '%obese class I%' THEN 'Obese Class I'
            WHEN value IS NULL AND code_description ILIKE '%obese class II%' THEN 'Obese Class II'
            WHEN value IS NULL AND code_description ILIKE '%obese class III%' THEN 'Obese Class III'
            WHEN value IS NULL AND code_description ILIKE '%obes%' THEN 'Obese Unclassified'
            
            ELSE 'Unknown' 
        END AS calculated_weight_class
    FROM coded_weight_class
),
latest_data AS (
    -- Get most recent clinical_effective_date per person
    SELECT 
        cd.*,
        pd.ethnicity,
        pd.imd_decile,
        pd.gender
    FROM categorised_data cd
    JOIN intelligence_dev.ai_centre_feature_store.person_nel_master_index pd 
        ON cd.person_id = pd.person_id
    WHERE pd.patient_status = 'ACTIVE' 
    AND cd.clinical_effective_date = (
        SELECT MAX(clinical_effective_date) 
        FROM categorised_data sub 
        WHERE sub.person_id = cd.person_id
    ) 
)
-- Select only the most recent entry per person
SELECT * FROM latest_data
where calculated_weight_class <> 'Unknown'
;

