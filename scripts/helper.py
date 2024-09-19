import os
import sys
import pandas as pd
from snowflake.snowpark import Session

def confirm_env_vars(env_vars):
    """
    Confirms that necessary environmental variables are set correctly.
        env_vars: list of env variable names to check.
    """
    for var in env_vars:
        if not os.getenv(var):
            print(f"Error: Environment variable {var} not set.")
            sys.exit(1)

def create_snowflake_session():
    """
    Creates a Snowflake session. Connection parameters pulls from environmental variables.
    """
    connection_parameters = {
        "account": os.getenv("SNOWFLAKE_SERVER"),
        "user": os.getenv("SNOWFLAKE_USER"),
        "role": os.getenv("SNOWFLAKE_USERGROUP"),
        "authenticator": "externalbrowser"  # AAD sign-in
    }

    try:
        session = Session.builder.configs(connection_parameters).create()
        print("Snowflake session created successfully.")
        return session
    except Exception as e:
        print(f"Error creating Snowflake session: {e}")
        sys.exit(1)

def select_database_schema(session, database, schema):
    """
    Sets the database and schema context for the Snowflake session.
        session: Snowflake session object
        database: name of the database to use
        schema: name of the schema to use
    """
    try:
        session.sql(f"USE DATABASE {database}").collect()
        session.sql(f"USE SCHEMA {schema}").collect()
        print(f"Using database: {database}, schema: {schema}")
    except Exception as e:
        print(f"Error connecting: {e}")
        sys.exit(1)

def load_csv_as_table(session, csv_path, table_name, database, schema):
    """
    Loads a CSV file as a pandas DataFrame and creates a table in Snowflake.
        session: Snowflake session
        csv_path: path to the CSV file
        table_name: name of the table to be created
        database: name of database to use
        schema: name of the schema to use
    """
    try:
        df = pd.read_csv(csv_path)
        
        temp_table_name = f"TEMP_{table_name}"
        session.create_dataframe(df).write.save_as_table(temp_table_name, mode="overwrite", table_type="temporary")
        
        table_query = f"""
        CREATE OR REPLACE TABLE {database}.{schema}.{table_name} AS
        SELECT * FROM {temp_table_name};
        """

        session.sql(table_query).collect()

        print(f"Table '{table_name}' created successfully in {database}.{schema}")

        try:
            query = f"SELECT * FROM {database}.{schema}.{table_name} LIMIT 10"
            result = session.sql(query).collect()
            print(f"Top 10 rows of {table_name}:")
            for row in result:
                print(row)
        except Exception as e:
            print(f"Error loading top 10 rows: {e}")
            sys.exit(1)

    except Exception as e:
        print(f"Error creating table from CSV: {e}")
        raise
import pandas as pd
import sys

def load_pq_as_table(session, parquet_path, table_name, database, schema):
    """
    Loads a local Parquet file as a pandas DataFrame and creates a table in Snowflake.
        session: Snowflake session
        parquet_path: local path to the Parquet file
        table_name: name of the table to be created
        database: name of database to use
        schema: name of the schema to use
    """
    try:
        df = pd.read_parquet(parquet_path)
        
        temp_table_name = f"TEMP_{table_name}"
        session.create_dataframe(df).write.save_as_table(temp_table_name, mode="overwrite", table_type="temporary")
        
        table_query = f"""
        CREATE OR REPLACE TABLE {database}.{schema}.{table_name} AS
        SELECT * FROM {temp_table_name};
        """
        
        session.sql(table_query).collect()

        print(f"Table '{table_name}' created successfully in {database}.{schema}")

        try:
            query = f"SELECT * FROM {database}.{schema}.{table_name} LIMIT 10"
            result = session.sql(query).collect()
            print(f"Top 10 rows of {table_name}:")
            for row in result:
                print(row)
        except Exception as e:
            print(f"Error loading top 10 rows: {e}")
            sys.exit(1)

    except Exception as e:
        print(f"Error creating table from Parquet: {e}")
        raise

def list_tables(session, database, schema):
    """
    Lists all tables in the specified database and schema.
        session: Snowflake session
        database: name of database to use
        schema: name of the schema to use
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