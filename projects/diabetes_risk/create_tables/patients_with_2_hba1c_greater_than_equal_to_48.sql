-- diagnose by hba1c
with patients_with_2_high_hba1cs as (
    SELECT a.person_id
    FROM INTELLIGENCE_DEV.AI_CENTRE_FEATURE_STORE.HBA1C_WITH_UNIT_REALLOCATION_V1 as a
    WHERE a.result_value_cleaned_and_converted_to_mmol_per_mol >= 48
    GROUP BY a.person_id
    HAVING COUNT(*) >= 2
)
select p.person_id, p.gender_concept_id, p.date_of_birth, p.date_of_death, p.date_of_death_inc_codes, p.current_address_id, p.ethnic_code_concept_id, p.approx_current_age
from prod_dwh.analyst_primary_care.patient as p
join patients_with_2_high_hba1cs as h
on h.person_id = p.person_id
group by p.person_id, p.gender_concept_id, p.date_of_birth, p.date_of_death, p.date_of_death_inc_codes, p.current_address_id, p.ethnic_code_concept_id, p.approx_current_age;