select p.person_id, p.gender_concept_id, p.date_of_birth, p.date_of_death, p.date_of_death_inc_codes, p.current_address_id, p.ethnic_code_concept_id, p.approx_current_age
from prod_dwh.analyst_primary_care.patient as p
join prod_dwh.analyst_primary_care.observation as o
on p.person_id = o.person_id
join prod_dwh.analyst_primary_care.concept as c
on o.core_concept_id = c.dbid
where c.code = '315051004'
group by p.person_id, p.gender_concept_id, p.date_of_birth, p.date_of_death, p.date_of_death_inc_codes, p.current_address_id, p.ethnic_code_concept_id, p.approx_current_age;