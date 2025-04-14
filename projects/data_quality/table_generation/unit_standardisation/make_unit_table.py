import json
import logging
import os
import re

import pandas as pd
from dotenv import load_dotenv

from phmlondon.snow_utils import SnowflakeConnection


def save_to_snowflake(snow: SnowflakeConnection, record: list[dict], table_name: str) -> None:
    """Function to pull out json, insert into table
    snow: Snowflake connection
    record: a list of dicts
    table_name: table to insert into"""

    # Check that data is a list of dictionaries
    if not isinstance(record, list) or not all(isinstance(item, dict) for item in record):
        print("Expected JSON file to contain a list of dictionaries.")
        return

    # Extract keys from the first dictionary to define table columns
    keys = list(record[0].keys())
    cursor = snow.session.connection.cursor()

    # Build the table schema dynamically based on JSON keys (all columns are TEXT)
    columns_str = ", ".join([f"{key} TEXT" for key in keys])
    create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns_str});"
    cursor.execute(create_table_query)

    # Prepare the INSERT statement with placeholders
    placeholders = ", ".join(["?"] * len(keys))
    insert_query = f"INSERT INTO {table_name} ({', '.join(keys)}) VALUES ({placeholders})"

    # Insert each record from the JSON data into the table
    for row in record:
        # Ensure ordering of values matches the column order
        values = [str(row.get(key, None)) for key in keys]
        cursor.execute(insert_query, values)


def main():
    load_dotenv()

    try:
        snowsesh = SnowflakeConnection()
        snowsesh.use_database("INTELLIGENCE_DEV")
        snowsesh.use_schema("AI_CENTRE_FEATURE_STORE")

        #Set some variables
        database = "INTELLIGENCE_DEV"
        schema = "AI_CENTRE_OBSERVATION_STAGING_TABLES"
        table = "UNIT_LOOKUP"
        unit_columns = ['RESULT_VALUE_UNITS', 'CLEANED_UNITS', 'DEFINITION_NAME']
        json_filename = 'unit_conversions.json'

        #Make the table if it doesn't exist
        snowsesh.execute_query(
            f"""CREATE TABLE IF NOT EXISTS
            {database}.{schema}.{table}
            (
            RESULT_VALUE_UNITS varchar NOT NULL,
            CLEANED_UNITS varchar NOT NULL,
            DEFINITION_NAME varchar NOT NULL
            )
            """)

        #Pull out the rows that aren't already there
        query = f"""SELECT *
        FROM {database}.{schema}.{table}"""
        current_table = snowsesh.execute_query_to_df(query)

        #Make sure there is something within current_table
        if current_table.shape == (0,0):
            current_table = pd.DataFrame(columns = unit_columns)

        #Compare what is in the json to what we have already in the table
        new_table = pd.read_json(json_filename)
        current_table['already_present'] = True
        combined_tabs = pd.merge(new_table,
                                 current_table,
                                 on = unit_columns,
                                 how = 'outer'
                                 )

        #Now pull out just the lines in the dict where this data isn't already

        #Find where the rows aren't already in the table
        new_dict = combined_tabs.loc[combined_tabs.already_present.isna(),
                                     unit_columns
                                     ].to_dict(orient='records')

        with open(json_filename, 'r+') as file:
            old_dict = json.load(file)

        missing_dict = [row for row in old_dict if row in new_dict]

        #Read in units json
        if len(missing_dict) > 0:
            save_to_snowflake(snowsesh, missing_dict, f'{database}.{schema}.{table}')
        else:
            print('No new mappings to upload.')

    except Exception as e:
        print(f"Error creating tables: {e}")
        raise e
    finally:
        snowsesh.session.close()

if __name__ == "__main__":
    main()