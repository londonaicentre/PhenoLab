--Table to convert between hb units - this one converts g/l to g/dl

        CREATE OR REPLACE TABLE INTELLIGENCE_DEV.AI_CENTRE_OBSERVATION_STAGING_TABLES.haemoglobin_VALUE_CUTOFFS AS
        SELECT 
        result_value as original_result_value,
        result_value_units as original_result_value_units,
        cleaned_units as standardised_units,
        cleaned_result_value as converted_result_value,
        CASE
        	WHEN cleaned_result_value < 25 then (((cleaned_result_value * 10 ) * 1 )  * 1 )
	
        ELSE CLEANED_RESULT_VALUE
        END
        as final_result_value,
        CASE
        	WHEN cleaned_result_value < 25 then FALSE 
	 	WHEN cleaned_result_value > 250 then FALSE 
	
        ELSE TRUE
        END 
        as final_result_value_confidence,
        CASE
        	WHEN cleaned_result_value < 2.5 then FALSE 
	 	WHEN cleaned_result_value > 250 then FALSE 
	
        ELSE TRUE
        END 
        as final_result_value_possible,
        observation_name,
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
        standardisation_last_updated,
        last_updated as conversion_last_updated,
        CURRENT_TIMESTAMP(0) as last_updated,
        NULL as table_version
        FROM INTELLIGENCE_DEV.AI_CENTRE_OBSERVATION_STAGING_TABLES.haemoglobin_UNITS_CONVERTED
        WHERE DEFINITION_NAME = 'haemoglobin_gp'