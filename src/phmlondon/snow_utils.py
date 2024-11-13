import os
import sys
import pandas as pd
from snowflake.snowpark import Session

class SnowflakeConnection:
    def __init__(self, env_vars=None):
        """
        Initialise SnowflakeConnection and connect.
            env_vars: list of required environment variable names
        """
        self.env_vars = env_vars or [
            "SNOWFLAKE_SERVER",
            "SNOWFLAKE_USER",
            "SNOWFLAKE_USERGROUP"
        ]
        self.session = None
        self.current_database = None
        self.current_schema = None
        
        self._confirm_env_vars()
        self._create_session()

    def close(self):
        """
        Close session
        """
        if self.session:
            try:
                self.session.close()
                print("Snowflake session closed.")
            except Exception as e:
                print(f"Error closing session")
                raise e

    def _confirm_env_vars(self):
        """
        Confirms that necessary env vars are set correctly.
        """
        for var in self.env_vars:
            if not os.getenv(var):
                print(f"Error: Environment variable {var} not set.")
                raise

    def _create_session(self):
        """
        Creates a Snowflake session using env vars
        """
        connection_parameters = {
            "account": os.getenv("SNOWFLAKE_SERVER"),
            "user": os.getenv("SNOWFLAKE_USER"),
            "role": os.getenv("SNOWFLAKE_USERGROUP"),
            "authenticator": "externalbrowser"
        }
        try:
            self.session = Session.builder.configs(connection_parameters).create()
            print("Snowflake session created successfully.")
        except Exception as e:
            print(f"Error creating Snowflake session.")
            raise e

    def use_database(self, database):
        """
        Sets the database for session
        """
        try:
            self.session.sql(f"USE DATABASE {database}").collect()
            self.current_database = database
            print(f"Using database: {database}")
        except Exception as e:
            print(f"Error setting database")
            raise e

    def use_schema(self, schema):
        """
        Sets the schema for session if desired
        """
        if not self.current_database:
            raise ValueError("Database must be set first. Try use_database()")
            
        try:
            self.session.sql(f"USE SCHEMA {schema}").collect()
            self.current_schema = schema
            print(f"Using schema: {schema}")
        except Exception as e:
            print(f"Error setting schema")
            raise e

    def load_csv_as_table(self, csv_path, table_name):
        """
        Loads a CSV file as a pandas DataFrame and creates a table in Snowflake.
            csv_path: path to the CSV file
            table_name: name of the table to be created
        """
        if not self.current_database or not self.current_schema:
            raise ValueError("Database and schema must be set before loading data")
            
        try:
            df = pd.read_csv(csv_path)
            
            temp_table_name = f"TEMP_{table_name}"
            self.session.create_dataframe(df).write.save_as_table(
                temp_table_name, 
                mode="overwrite", 
                table_type="temporary"
            )
            
            table_query = f"""
            CREATE OR REPLACE TABLE {self.current_database}.{self.current_schema}.{table_name} AS
            SELECT * FROM {temp_table_name};
            """

            self.session.sql(table_query).collect()
            print(f"Table '{table_name}' created successfully in {self.current_database}.{self.current_schema}")
            self.preview_table(table_name)

        except Exception as e:
            print(f"Error creating table from CSV: {e}")
            raise

    def load_parquet_as_table(self, parquet_path, table_name):
        """
        Loads a Parquet file as a pandas DataFrame and creates a table in Snowflake.
            parquet_path: path to the Parquet file
            table_name: name of the table to be created
        """
        if not self.current_database or not self.current_schema:
            raise ValueError("Database and schema must be set before loading data")
            
        try:
            df = pd.read_parquet(parquet_path)
            
            temp_table_name = f"TEMP_{table_name}"
            self.session.create_dataframe(df).write.save_as_table(
                temp_table_name, 
                mode="overwrite", 
                table_type="temporary"
            )
            
            table_query = f"""
            CREATE OR REPLACE TABLE {self.current_database}.{self.current_schema}.{table_name} AS
            SELECT * FROM {temp_table_name};
            """

            self.session.sql(table_query).collect()
            print(f"Table '{table_name}' created successfully in {self.current_database}.{self.current_schema}")
            self.preview_table(table_name)

        except Exception as e:
            print(f"Error creating table from Parquet: {e}")
            raise

    def preview_table(self, table_name, limit=10):
        """
        Shows the first few rows of a table.
            table_name: name of the table to preview
            limit: number of rows to show (default: 10)
        """
        try:
            query = f"SELECT * FROM {self.current_database}.{self.current_schema}.{table_name} LIMIT {limit}"
            result = self.session.sql(query).collect()
            print(f"Top {limit} rows of {table_name}:")
            for row in result:
                print(row)
        except Exception as e:
            print(f"Error loading preview")
            raise e

    def list_tables(self):
        """
        Lists all tables in the current database and schema.
        """
        if not self.current_database or not self.current_schema:
            raise ValueError("Database and schema must be set before listing tables")
            
        try:
            query = f"SHOW TABLES IN {self.current_database}.{self.current_schema}"
            result = self.session.sql(query).collect()
            
            table_names = [row['name'] for row in result]
            
            print(f"Tables in {self.current_database}.{self.current_schema}:")
            for table in table_names:
                print(f"- {table}")
                
            return table_names
        except Exception as e:
            print(f"Error listing tables")
            raise e
