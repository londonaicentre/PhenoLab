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
max_date AS (
    SELECT person_id, MAX(clinical_effective_date) AS max_date
    FROM patients_with_2_spaced_highs
    GROUP BY person_id  
)
SELECT 
    p.person_id,
    p.gender_concept_id,
    p.date_of_birth,
    p.date_of_death,
    p.date_of_death_inc_codes,
    p.current_address_id,
    p.ethnic_code_concept_id,
    p.approx_current_age,
    h.clinical_effective_date,
    h.hba1c_value
FROM patients_with_2_spaced_highs AS h
JOIN max_date AS m
  ON h.person_id = m.person_id
  AND h.clinical_effective_date = m.max_date
JOIN prod_dwh.analyst_primary_care.patient AS p
  ON h.person_id = p.person_id;
