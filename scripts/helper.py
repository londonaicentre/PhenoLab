import os
import sys
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
        print(f"Error using database or schema: {e}")
        sys.exit(1)