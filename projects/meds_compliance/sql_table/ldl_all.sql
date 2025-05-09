CREATE OR REPLACE TABLE intelligence_dev.ai_centre_dev.ldl_all AS
SELECT 
o.person_id,
o.age_at_event,
o.clinical_effective_date as result_date,
d.code_description,
o.result_value,
o.result_value_units
FROM prod_dwh.analyst_primary_care.observation o
JOIN intelligence_dev.ai_centre_definition_library.definitionstore d
  ON d.dbid = o.core_concept_id
  AND d.definition_id ILIKE '999018771000230107' -- LDL

  ;