--Table to convert between hb units - this one converts g/l to g/dl

        CREATE OR REPLACE TABLE INTELLIGENCE_DEV.AI_CENTRE_OBSERVATION_STAGING_TABLES.haemoglobin_UNITS_CONVERTED AS
        SELECT 
        result_value,
        result_value_units,
        cleaned_units,
        CASE
        	WHEN cleaned_units = 'g/dl' then (((result_value * 10 ) * 1 ) * 1 )
	 	WHEN cleaned_units = 'g/l' then (((result_value * 1 ) * 1 ) * 1 )
	
        END
        as cleaned_result_value,
        CASE
        	WHEN cleaned_units = 'g/dl' then 'g/dl'
	 	WHEN cleaned_units = 'g/l' then 'g/dl'
	
        END
        as cleaned_result_value_units,
        'hb' as observation_name,
        id,
        organization_id, 
        patient_id,
        person_id,
        encounter_id,
        date_recorded,
        clinical_effective_date,
        core_concept_id,
        code_description,
        definition_name,
        definition_version,
        definition_source,
        version_datetime,
        last_updated as standardisation_last_updated,
        CURRENT_TIMESTAMP(0) as last_updated,
        NULL as table_version
        FROM INTELLIGENCE_DEV.AI_CENTRE_OBSERVATION_STAGING_TABLES.haemoglobin_UNITS_STANDARDISED
        WHERE DEFINITION_NAME = 'haemoglobin_gp'