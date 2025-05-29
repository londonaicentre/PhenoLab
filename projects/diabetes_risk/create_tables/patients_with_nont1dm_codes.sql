WITH ranked_codes AS (
  SELECT 
    p.person_id, 
    p.gender_concept_id, 
    p.date_of_birth, 
    p.date_of_death, 
    p.date_of_death_inc_codes, 
    p.current_address_id, 
    p.ethnic_code_concept_id, 
    p.approx_current_age, 
    o.clinical_effective_date,
    o.core_concept_id,
    ROW_NUMBER() OVER (PARTITION BY p.person_id ORDER BY o.clinical_effective_date DESC) AS rn_desc,
    ROW_NUMBER() OVER (PARTITION BY p.person_id ORDER BY o.clinical_effective_date ASC) AS rn_asc
  FROM prod_dwh.analyst_primary_care.patient AS p
  JOIN prod_dwh.analyst_primary_care.observation AS o
    ON p.person_id = o.person_id
  JOIN prod_dwh.analyst_primary_care.concept AS c
    on o.core_concept_id = c.dbid
    join intelligence_dev.ai_centre_definition_library.definitionstore as ds
    on c.code = ds.code
    where ds.definition_id = '7b322f7f'
),
earliest AS (
  SELECT person_id, clinical_effective_date AS earliest_diagnosis_date
  FROM ranked_codes
  WHERE rn_asc = 1
),
latest AS (
  SELECT 
    person_id, 
    gender_concept_id, 
    date_of_birth, 
    date_of_death, 
    date_of_death_inc_codes, 
    current_address_id, 
    ethnic_code_concept_id, 
    approx_current_age, 
    clinical_effective_date AS latest_diagnosis_date,
    core_concept_id
  FROM ranked_codes
  WHERE rn_desc = 1
)
SELECT 
  l.person_id, 
  l.gender_concept_id, 
  l.date_of_birth, 
  l.date_of_death, 
  l.date_of_death_inc_codes, 
  l.current_address_id, 
  l.ethnic_code_concept_id, 
  l.approx_current_age, 
  l.latest_diagnosis_date,
  e.earliest_diagnosis_date,
  l.core_concept_id
FROM latest l
JOIN earliest e ON l.person_id = e.person_id;