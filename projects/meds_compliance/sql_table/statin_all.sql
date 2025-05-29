CREATE OR REPLACE TABLE intelligence_dev.ai_centre_dev.statin_all AS
select 
o.person_id,
o.age_at_event,
o.clinical_effective_date as order_date,
d.codelist_name as drug_name,
o.dose,
o.duration_days,
o.quantity_unit,
o.quantity_value
from prod_dwh.analyst_primary_care.medication_order o
left join intelligence_dev.ai_centre_definition_library.definitionstore d
on d.dbid = o.core_concept_id
where definition_id = '212000' -- lipid regulating drugs
;