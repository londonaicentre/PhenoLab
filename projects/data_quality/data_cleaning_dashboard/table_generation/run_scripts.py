import os
import re

from dotenv import load_dotenv
from make_union_table import main as union_table
from unit_conversion.make_tables import main as unit_conversion
from unit_standardisation.make_tables import main as unit_standardisation
from unit_standardisation.make_unit_table import main as unit_table
from value_cutoffs.make_tables import main as value_cutoffs

from phmlondon.snow_utils import SnowflakeConnection


def get_sql_files(directory = None) -> list:
    """Returns all the sql files in the directory"""
    files =  os.listdir(directory)
    return [file for file in files if re.findall('.*\\.sql', file)]


def main():
    load_dotenv()

    #Make the sql tables
    unit_standardisation('unit_standardisation')
    unit_conversion('unit_conversion')
    value_cutoffs('value_cutoffs')

    try:
        snowsesh = SnowflakeConnection()
        snowsesh.use_database("INTELLIGENCE_DEV")
        snowsesh.use_schema("AI_CENTRE_FEATURE_STORE")

        #Make the unit lookup table
        unit_table('unit_standardisation')

        #Get the sql files in the different folders and work through them
        directories = ['unit_standardisation', 'unit_conversion', 'value_cutoffs']
        for dir in directories:
            dir = os.path.join(dir, 'sql_scripts')
            files = get_sql_files(dir)

            #Run all the files
            for file in files:
                file = os.path.join(dir, file)
                snowsesh.execute_sql_file(file)
                print(f'SQL script {file} successfully run')

        print('Unit standardisation tables created')

        #Now union all the table
        snowsesh.execute_sql_file('union_all_cleaned_observations.sql')

        #Finally union together all the tables
        union_table()

    except Exception as e:
        print(f"Error creating tables: {e}")
        raise e
    finally:
        snowsesh.session.close()

if __name__ == "__main__":
    main()
