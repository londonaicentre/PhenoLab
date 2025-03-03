import os
import sys
import pandas as pd
import polars as pl
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
            print(f"Error setting database: {e}")
            raise e

    def use_schema(self, schema):
        """
        Sets the schema for session if desired
        """
        if not self.current_database:
            raise ValueError("Database must be set first. Try use_database(database_name)")

        try:
            self.session.sql(f"USE SCHEMA {schema}").collect()
            self.current_schema = schema
            print(f"Using schema: {schema}")
        except Exception as e:
            print(f"Error setting schema {e}")
            raise e

    def _validate_database_schema(self, schema_required=True):
        """
        Internal method to validate database and schema are set
            schema_required (bool): If true validates db and schema
        """
        if not self.current_database:
            raise ValueError("Database must be set first. Try use_database(database_name)")

        if schema_required and not self.current_schema:
            raise ValueError("Schema must be set first. Try use_schema(schema_name)")

    def _load_dataframe_to_snowflake(self, df, table_name, mode="overwrite", table_type=""):
        """
        Internal method to handle loading of DataFrames to Snowflake with standardized options.
            df: dataFrame to load
            table_name: target table name
            mode: "overwrite" or "append"
            table_type: "temporary" (note empty string = permanent)
        """
        self._validate_database_schema(schema_required=True)

        # https://docs.snowflake.com/en/developer-guide/snowpark/reference/python/latest/snowpark/api/snowflake.snowpark.DataFrameWriter.save_as_table
        valid_modes = ["overwrite", "append"]
        valid_table_types = ["", "temporary"]

        if mode not in valid_modes:
            raise ValueError(f"Invalid mode, please use: {valid_modes}")
        if table_type not in valid_table_types:
            raise ValueError(f"Invalid table type, please use: {valid_table_types}")

        try:
            self.session.create_dataframe(df).write.save_as_table(
                table_name,
                mode=mode,
                table_type=table_type
            )
            print(f"Data loaded successfully")

            return table_name

        except Exception as e:
            print(f"Error loading dataframe to snowflake table: {e}")
            raise e

    def load_csv_as_table(self, csv_path, table_name, mode="overwrite", table_type=""):
        """
        Loads a CSV file as a table in Snowflake.
            csv_path: path to the CSV file
            table_name: name of target table
            mode: "overwrite" or "append"
            table_type: "temporary", empty string is permanent
        """
        try:
            df = pd.read_csv(csv_path)
            return self._load_dataframe_to_snowflake(
                df=df,
                table_name=table_name,
                mode=mode,
                table_type=table_type,
            )
        except Exception as e:
            print(f"Error loading CSV file: {e}")
            raise e

    def load_parquet_as_table(self, parquet_path, table_name, mode="overwrite", table_type=""):
        """
        Loads a parquet file as a table in Snowflake.
            parquet_path: path to the Parquet file
            table_name: name of target table
            mode: "overwrite" or "append"
            table_type: "temporary", empty string is permanent
        """
        try:
            df = pd.read_parquet(parquet_path)
            return self._load_dataframe_to_snowflake(
                df=df,
                table_name=table_name,
                mode=mode,
                table_type=table_type,
            )
        except Exception as e:
            print(f"Error loading Parquet file: {e}")
            raise e

    def load_dataframe_to_table(self, df, table_name, mode="overwrite", table_type=""):
        """
        Loads a pandas DataFrame directly into snowflake table.
            df: dataFrame to load
            table_name: name of target table
            mode: "overwrite" or "append"
            table_type: "temporary", empty string is permanent
        """
        return self._load_dataframe_to_snowflake(
            df=df,
            table_name=table_name,
            mode=mode,
            table_type=table_type,
        )

    def preview_table(self, table_name, limit=10):
        """
        Shows the first few rows of a table.
            table_name: name of the table to preview
            limit: number of rows to show (default: 10)
        """
        self._validate_database_schema(schema_required=True)

        try:
            query = f"SELECT * FROM {self.current_database}.{self.current_schema}.{table_name} LIMIT {limit}"
            result = self.session.sql(query).collect()
            print(f"Top {limit} rows of {table_name}:")
            for row in result:
                print(row)
        except Exception as e:
            print(f"Error loading preview: {e}")
            raise e

    def list_tables(self):
        """
        Lists all tables in the current database and schema.
        """
        self._validate_database_schema(schema_required=True)

        try:
            query = f"SHOW TABLES IN {self.current_database}.{self.current_schema}"
            result = self.session.sql(query).collect()

            table_names = [row['name'] for row in result]

            print(f"Tables in {self.current_database}.{self.current_schema}:")
            for table in table_names:
                print(f"- {table}")

            return table_names
        except Exception as e:
            print(f"Error listing tables: {e}")
            raise e

    def execute_query(self, query):
        """
        Executes a SQL query without returning results
            query: predefined query
        """
        try:
            self.session.sql(query).collect()
            print("Query executed successfully.")
        except Exception as e:
            print(f"Error executing query :{e}")
            raise e

    def execute_query_to_df(self, query):
        """
        Executes a SQL query and returns results as a pandas DataFrame
            query: predefined query
        """
        try:
            result = self.session.sql(query).collect()
            return pd.DataFrame(result)
        except Exception as e:
            print(f"Error executing query: {e}")
            raise e
        
    def execute_query_to_polars(self, query: str) -> pl.DataFrame:
        """
        Executes a SQL query and returns results as a polars DataFrame
            query: predefined query
        """
        try:
            with self.session.connection.cursor() as cur:
                # Execute a query in polars
                cur.execute(query)
                arrow_tab = cur.fetch_arrow_all()
                return pl.from_arrow(arrow_tab)
    
        except Exception as e:
            print(f"Error executing query: {e}")
            raise e

    def execute_sql_file(self, sql_file_path):
        """
        Reads and executes a sql file.
            sql_file_path: path to the sql file
        Note: this splits by ';' because snowpark execute_query doesn't support multi-part queries.
        This will break if individual queries contain ';', e.g. in strings
        """
        try:
            with open(sql_file_path, 'r') as file:
                sql_commands = file.read()

            commands = sql_commands.split(';')

            for command in commands:
                if command.strip():
                    self.execute_query(command)

            print(f"'{sql_file_path}' executed successfully.")

        except FileNotFoundError as f:
            print(f"Unable to find sql file: {f}")
            raise
        except Exception as e:
            print(f"Error executing sql file: {e}")
            raise