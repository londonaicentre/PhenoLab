--Table to standardise hb unit conversions - this one standardises to g/l and g/dl

        CREATE OR REPLACE TABLE INTELLIGENCE_DEV.AI_CENTRE_OBSERVATION_STAGING_TABLES.haemoglobin_UNITS_STANDARDISED AS
        SELECT 
        obs.result_value,
        obs.result_value_units,
        CASE
        	WHEN REGEXP_LIKE(obs.result_value_units, '.*(g.*dl).*', 'i') then 'g/dl'
	 	WHEN REGEXP_LIKE(obs.result_value_units, '.*(g.{0,2}l).*', 'i') then 'g/l'
	
        END
        as cleaned_units,
        'hb' as observation_name,
        obs.id,
        obs.organization_id, 
        obs.patient_id,
        obs.person_id,
        obs.encounter_id,
        obs.date_recorded,
        obs.clinical_effective_date,
        obs.core_concept_id,
        def.code_description,
        def.definition_name,
        def.definition_version,
        def.definition_source,
        def.version_datetime,
        CURRENT_TIMESTAMP(0) as last_updated,
        NULL as table_version
        FROM PROD_DWH.ANALYST_PRIMARY_CARE.OBSERVATION obs
        left join intelligence_dev.ai_centre_definition_library.definitionstore def on obs.core_concept_id = def.dbid
        WHERE def.code is not null
        and def.DEFINITION_NAME = 'haemoglobin_gp'