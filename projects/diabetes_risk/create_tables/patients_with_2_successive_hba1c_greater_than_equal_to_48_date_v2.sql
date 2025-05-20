WITH ordered_readings AS (
    SELECT
        person_id,
        result_value_cleaned_and_converted_to_mmol_per_mol AS hba1c_value,
        clinical_effective_date,
        LAG(result_value_cleaned_and_converted_to_mmol_per_mol) OVER (PARTITION BY person_id ORDER BY clinical_effective_date) AS prev_hba1c_value,
        LAG(clinical_effective_date) OVER (PARTITION BY person_id ORDER BY clinical_effective_date) AS prev_date
    FROM INTELLIGENCE_DEV.AI_CENTRE_FEATURE_STORE.HBA1C_WITH_UNIT_REALLOCATION_V1
),
patients_with_2_spaced_highs AS (
    SELECT person_id,
        clinical_effective_date,
        hba1c_value
    FROM ordered_readings
    WHERE hba1c_value >= 48
      AND prev_hba1c_value >= 48
      AND DATEDIFF(DAY, prev_date, clinical_effective_date) > 14
    GROUP BY person_id, clinical_effective_date, hba1c_value
),
ranked_highs AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY clinical_effective_date DESC) AS rn_desc,
           ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY clinical_effective_date ASC) AS rn_asc
    FROM patients_with_2_spaced_highs
),
earliest AS (
    SELECT person_id, clinical_effective_date AS earliest_diagnosis_date
    FROM ranked_highs
    WHERE rn_asc = 1
),
latest AS (
    SELECT person_id, clinical_effective_date AS latest_diagnosis_date
    FROM ranked_highs
    WHERE rn_desc = 1
)
SELECT 
    e.person_id,
    e.earliest_diagnosis_date,
    l.latest_diagnosis_date
FROM latest l
JOIN earliest e ON l.person_id = e.person_id;