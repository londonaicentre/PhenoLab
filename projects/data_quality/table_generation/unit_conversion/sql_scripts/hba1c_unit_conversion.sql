--Table to convert between HbA1c units, this one converts % (DCCT) to mmol/mol (IFCC)

        CREATE OR REPLACE TABLE INTELLIGENCE_DEV.AI_CENTRE_OBSERVATION_STAGING_TABLES.hba1c_UNITS_CONVERTED AS
        SELECT 
        result_value,
        result_value_units,
        cleaned_units,
        CASE
        	WHEN cleaned_units = 'mmol/mol' then (((result_value * 1 ) * 1 ) * 1 )
	 	WHEN cleaned_units = '%' then (((result_value - 2.15 ) * 10.929 )  * 1 )
	
        END
        as cleaned_result_value,
        CASE
        	WHEN cleaned_units = 'mmol/mol' then 'mmol/mol'
	 	WHEN cleaned_units = '%' then 'mmol/mol'
	
        END
        as cleaned_result_value_units,
        'hba1c' as observation_name,
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
        FROM INTELLIGENCE_DEV.AI_CENTRE_OBSERVATION_STAGING_TABLES.hba1c_UNITS_STANDARDISED
        WHERE DEFINITION_NAME = 'hba1c_definition_gp'