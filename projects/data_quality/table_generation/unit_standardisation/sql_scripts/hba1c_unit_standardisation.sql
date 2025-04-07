--Table to standardise hba1c unit conversions - this one standardises to mmol/mol (IFCC) and % (DCCT)

        CREATE OR REPLACE TABLE INTELLIGENCE_DEV.AI_CENTRE_OBSERVATION_STAGING_TABLES.hba1c_UNITS_STANDARDISED AS
        SELECT 
        obs.result_value,
        obs.result_value_units,
        CASE
        	WHEN REGEXP_LIKE(obs.result_value_units, '.*(mmol|mM/M|IFCC).*', 'i') then 'mmol/mol'
	 	WHEN REGEXP_LIKE(obs.result_value_units, '.*(%|per[ -]?cent|DCCT).*', 'i') then '%'
	
        END
        as cleaned_units,
        'hba1c' as observation_name,
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
        and def.DEFINITION_NAME = 'hba1c_definition_gp'