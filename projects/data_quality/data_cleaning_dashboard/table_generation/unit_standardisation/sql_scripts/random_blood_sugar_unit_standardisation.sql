--Table to standardise random blood sugar units - this one does mmol/l and mg/dl

        CREATE OR REPLACE TABLE INTELLIGENCE_DEV.AI_CENTRE_OBSERVATION_STAGING_TABLES.random_blood_sugar_UNITS_STANDARDISED AS
        SELECT
            obs.result_value,
            obs.result_value_units,
            units.cleaned_units,
            'random_blood_sugar' as observation_name,
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
            left join INTELLIGENCE_DEV.AI_CENTRE_OBSERVATION_STAGING_TABLES.UNIT_LOOKUP units
                ON obs.result_value_units = units.result_value_units
                AND def.definition_name = units.definition_name
            WHERE def.code is not null
            and def.DEFINITION_NAME = 'random_blood_sugar_gp'