import json
import os


def main():
    #Read in the conversion file
    with open('conversions.json', 'r+') as connection:
        conversions = json.load(connection)
    tables = [i for i in conversions.keys()]
    for table in tables:

        #Make each query
        config = conversions[table]
        query = f"""--{config['comment']}\n
        CREATE OR REPLACE TABLE INTELLIGENCE_DEV.{config['schema']}.{table}_UNITS_CONVERTED AS
        SELECT 
        result_value,
        result_value_units,
        cleaned_units,
        CASE
        {' '.join('\tWHEN cleaned_units = \'' + unit + '\' then (((result_value ' + conversion[0] + ' ) ' + conversion[1] + ' ) ' + conversion[2] + ' )\n\t'
                  for unit, conversion in config['unit_conversions'].items())}
        END
        as cleaned_result_value,
        CASE
        {' '.join('\tWHEN cleaned_units = \'' + old_unit + '\' then \'' + new_unit + '\'\n\t'
                  for old_unit, new_unit in config['unit_changes'].items())}
        END
        as cleaned_result_value_units,
        '{config['observation_name']}' as observation_name,
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
        FROM INTELLIGENCE_DEV.{config['schema']}.{table}_UNITS_STANDARDISED
        WHERE DEFINITION_NAME = '{config['definition']}'"""  # noqa: E501

        with open(os.path.join('sql_scripts', table + '_unit_conversion.sql'), 'w') as file:
            file.write(query)

        print(f'Made table {table}_units_converted')

if __name__ == "__main__":
    main()
