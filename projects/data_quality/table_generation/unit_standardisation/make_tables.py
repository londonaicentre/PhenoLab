import json
import os


def main():
    #Read in the conversion file
    with open('standardisations.json', 'r+') as connection:
        conversions = json.load(connection)
    tables = [i for i in conversions.keys()]

    for table in tables:

        #Make each query
        config = conversions[table]
        query = f"""--{config['comment']}\n
        CREATE OR REPLACE TABLE INTELLIGENCE_DEV.{config['schema']}.{table}_UNITS_STANDARDISED AS
        SELECT
            obs.result_value,
            obs.result_value_units,
            units.cleaned_units,
            '{config['observation_name']}' as observation_name,
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
            left join INTELLIGENCE_DEV.{config['schema']}.{config['unit_table']} units
                ON obs.result_value_units = units.result_value_units
                AND def.definition_name = units.definition_name
            WHERE def.code is not null
            and def.DEFINITION_NAME = '{config['definition']}'"""

        with open(os.path.join('sql_scripts', table + '_unit_standardisation.sql'), 'w') as file:
            file.write(query)

        print(f'Made table {table}_units_standardised')

if __name__ == "__main__":
    main()
