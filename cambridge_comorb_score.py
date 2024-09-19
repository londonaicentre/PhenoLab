import os
import pandas as pd
from dotenv import load_dotenv
from scripts.helper import confirm_env_vars, create_snowflake_session, select_database_schema, load_csv_as_table
import sys

def list_tables(session, database, schema):
    """
    Lists all tables in the specified database and schema.
    """
    try:
        query = f"SHOW TABLES IN {database}.{schema}"
        result = session.sql(query).collect()
        
        table_names = [row['name'] for row in result]
        
        print(f"Tables in {database}.{schema}:")
        for table in table_names:
            print(f"- {table}")
    except Exception as e:
        print(f"Error listing tables: {e}")
        sys.exit(1)

def main():
    load_dotenv()

    env_vars = ["SNOWFLAKE_SERVER", "SNOWFLAKE_USER", "SNOWFLAKE_USERGROUP"]
    confirm_env_vars(env_vars)

    # Set up Snowflake connection
    session = create_snowflake_session()
    
    database = "INTELLIGENCE_DEV"
    schema = "AI_CENTRE_DEV"
    select_database_schema(session, database, schema)
    
    list_tables(session, database, schema)

    # Load CSVs from 2022 paper
    load_csv_as_table(session, "data/camb_comorb_score/camb_comorb_snomed.csv", "CAMBRIDGE_COMORB_2022_SNOMED", database, schema)
    load_csv_as_table(session, "data/camb_comorb_score/camb_comorb_dm+d.csv", "CAMBRIDGE_COMORB_2022_DMD", database, schema)
    load_csv_as_table(session, "data/camb_comorb_score/camb_comorb_emismed.csv", "CAMBRIDGE_COMORB_2022_EMISMED", database, schema)

    list_tables(session, database, schema)

if __name__ == "__main__":
    main()