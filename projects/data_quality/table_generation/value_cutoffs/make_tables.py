import json
import os


def main():
    #Read in the conversion file
    with open('cutoffs.json', 'r+') as connection:
        conversions = json.load(connection)
    tables = [i for i in conversions.keys()]
    for table in tables:

        #Make each query
        config = conversions[table]
        query = f"""--{config['comment']}\n
        CREATE OR REPLACE TABLE INTELLIGENCE_DEV.{config['schema']}.{table}_VALUE_CUTOFFS AS
        SELECT 
        result_value as original_result_value,
        result_value_units as original_result_value_units,
        cleaned_units as standardised_units,
        cleaned_result_value as converted_result_value,
        CASE
        {' '.join('\tWHEN cleaned_result_value ' + threshold +
                  ' then (((cleaned_result_value + ' + str(conversion[0]) + ' ) *' + str(conversion[1]) + ' ) +' + str(conversion[2]) + ' )\n\t'
                  for threshold, conversion in config['value_interval_conversions'].items())}
        ELSE CLEANED_RESULT_VALUE
        END
        as final_result_value,
        CASE
        {' '.join('\tWHEN cleaned_result_value ' + threshold + ' then FALSE \n\t'
                  for threshold in config['value_confidence'])}
        ELSE TRUE
        END 
        as final_result_value_confidence,
        CASE
        {' '.join('\tWHEN cleaned_result_value ' + threshold + ' then FALSE \n\t'
                  for threshold in config['value_possible'])}
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
        FROM INTELLIGENCE_DEV.{config['schema']}.{table}_UNITS_CONVERTED
        WHERE DEFINITION_NAME = '{config['definition']}'"""  # noqa: E501

        with open(os.path.join('sql_scripts', table + '_value_cutoffs.sql'), 'w') as file:
            file.write(query)

        print(f'Made table {table}_units_converted')

if __name__ == "__main__":
    main()
