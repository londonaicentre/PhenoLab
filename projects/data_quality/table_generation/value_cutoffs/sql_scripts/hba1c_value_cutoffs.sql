--Table to convert between HbA1c units, this one converts % (DCCT) to mmol/mol (IFCC)

        CREATE OR REPLACE TABLE INTELLIGENCE_DEV.AI_CENTRE_OBSERVATION_STAGING_TABLES.hba1c_VALUE_CUTOFFS AS
        SELECT 
        result_value as original_result_value,
        result_value_units as original_result_value_units,
        cleaned_units as standardised_units,
        cleaned_result_value as converted_result_value,
        CASE
        	WHEN cleaned_result_value BETWEEN 3 AND 15 then (((cleaned_result_value - 2.15 ) * 10.929 )  * 1 )
	
        ELSE CLEANED_RESULT_VALUE
        END
        as final_result_value,
        CASE
        	WHEN cleaned_result_value < 19 then FALSE 
	 	WHEN cleaned_result_value > 200 then FALSE 
	
        ELSE TRUE
        END 
        as final_result_value_confidence,
        CASE
        	WHEN cleaned_result_value < 3 then FALSE 
	 	WHEN cleaned_result_value BETWEEN 15 and 19 then FALSE 
	 	WHEN cleaned_result_value > 200 then FALSE 
	
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
        FROM INTELLIGENCE_DEV.AI_CENTRE_OBSERVATION_STAGING_TABLES.hba1c_UNITS_CONVERTED
        WHERE DEFINITION_NAME = 'hba1c_definition_gp'