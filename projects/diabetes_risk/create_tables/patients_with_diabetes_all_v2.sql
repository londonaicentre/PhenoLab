WITH valid_non_t1dm AS (
    SELECT 
        p.person_id,
        p.date_of_birth,
        p.gender_concept_id,
        p.core_concept_id, 
        1 AS diagnosis_by_code,
        0 AS diagnosis_by_hba1c,
        CAST(NULL AS DATE) AS earliest_hba1c_diagnosis_date,
        earliest_diagnosis_date as earliest_code_diagnosis_date
    FROM INTELLIGENCE_DEV.AI_CENTRE_FEATURE_STORE.PATIENTS_WITH_NON_T1DM_CODES_V1 p
    LEFT JOIN INTELLIGENCE_DEV.AI_CENTRE_FEATURE_STORE.PATIENTS_WITH_DIABETES_RESOLUTION_CODE_V1 r
        ON p.person_id = r.person_id
       AND r.clinical_effective_date > p.latest_diagnosis_date
    WHERE r.person_id IS NULL
),
valid_hba1c AS (
    SELECT 
        h.person_id,
        p.date_of_birth,
        p.gender_concept_id,
        NULL AS core_concept_id,
        0 AS diagnosis_by_code,
        1 AS diagnosis_by_hba1c,
        earliest_diagnosis_date as earliest_hba1c_diagnosis_date,
        CAST(NULL AS DATE) AS earliest_code_diagnosis_date
    FROM INTELLIGENCE_DEV.AI_CENTRE_FEATURE_STORE.PATIENTS_WITH_2_HBA1C_GREATER_THAN_EQUAL_TO_48_V1 h
    JOIN prod_dwh.analyst_primary_care.patient p
        ON h.person_id = p.person_id
    LEFT JOIN INTELLIGENCE_DEV.AI_CENTRE_FEATURE_STORE.PATIENTS_WITH_DIABETES_RESOLUTION_CODE_V1 r
        ON h.person_id = r.person_id
       AND r.clinical_effective_date > h.latest_diagnosis_date
    WHERE r.person_id IS NULL
      AND h.person_id NOT IN (
          SELECT person_id FROM valid_non_t1dm
      )
),
all_pts AS (
    SELECT * FROM valid_non_t1dm
    UNION
    SELECT * FROM valid_hba1c
)
SELECT
    a.*,
    CASE WHEN a.gender_concept_id = 1335244 THEN 1 ELSE 0 END AS gender_male,
    CASE WHEN a.gender_concept_id = 1335245 THEN 1 ELSE 0 END AS gender_female,
    p.ethnic_aic_category, -- white, mixed, east or other asian, south asian, black, mixed, [not stated, other, unknown]
    case
        when p.ethnic_aic_category = 'White' then 1
        else 0
    end as ethnicity_white,
    case 
        when p.ethnic_aic_category = 'Mixed' then 1
        else 0
    end as ethnicity_mixed,
    case 
        when p.ethnic_aic_category = 'East or Other Asian' then 1
        else 0
    end as ethnicity_east_or_other_asian,
    case 
        when p.ethnic_aic_category = 'Black' then 1
        else 0
    end as ethnicity_black,
    case
        when p.ethnic_aic_category = 'South Asian' then 1
        else 0
    end as ethnicity_south_asian,
    p.imd_decile,
    p.imd_quintile,
    p.smoking_status,
    p.latest_smoking_status_date,
    -- this comes from NEL's PMI table - don't know if should check it by calculating myself?
    case when p.smoking_status = 'Never Smoked' then 1 else 0 end as ss_never_smoker,
    case when p.smoking_status = 'Ex-Smoker' then 1 else 0 end as ss_ex_smoker,
    case when p.smoking_status = 'Current Smoker' then 1 else 0 end as ss_current_smoker,
    case 
        when a.earliest_hba1c_diagnosis_date is null then a.earliest_code_diagnosis_date
        when a.earliest_code_diagnosis_date is null then a.earliest_hba1c_diagnosis_date
        else least(a.earliest_hba1c_diagnosis_date, a.earliest_code_diagnosis_date) 
    end AS earliest_diagnosis_date_combined,
    datediff(year, a.date_of_birth, earliest_diagnosis_date_combined) as age_at_first_diagnosis
FROM all_pts a
JOIN INTELLIGENCE_DEV.AI_CENTRE_FEATURE_STORE.PERSON_MASTER_INDEX_V1 p
    ON a.person_id = p.person_id;
