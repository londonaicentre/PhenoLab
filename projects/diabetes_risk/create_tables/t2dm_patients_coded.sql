-- (1) Adult patients with T2DM (via code)
create table if not exists intelligence_dev.ai_centre_feature_store.t2dm_patients_coded_as as
select 
    p.person_id as person_id, 
    min(p.date_of_birth) as dob, 
    max(p.date_of_death) as dod,
    max(p.date_of_death_inc_codes) as dod_inc_codes,
    max(p.approx_current_age) as approx_current_age,
    max(gender_concept_id) as gender_concept_id,
    max(ethnic_code_concept_id) as ethnic_code_concept_id
from prod_dwh.analyst_primary_care.patient as p
join prod_dwh.analyst_primary_care.observation as o 
on p.person_id = o.person_id
join prod_dwh.analyst_primary_care.concept as c
on o.core_concept_id = c.dbid
join intelligence_dev.ai_centre_phenotype_library.phenostore as ps
on c.code = ps.code
where ps.phenotype_id = 'nhsd-primary-care-domain-refsets/dmnontype1_cod'
group by p.person_id
order by p.person_id;
