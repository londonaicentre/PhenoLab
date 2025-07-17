import pandas as pd
from snowflake.connector.errors import ProgrammingError
from snowflake.snowpark import Session


def create_definition_table(session: Session, table_name: str,
        database: str = "INTELLIGENCE_DEV", schema: str = "AI_CENTRE_DEFINITION_LIBRARY"):
    """
    Creates a definition table if it doesn't exist.
    Args:
        session:
            Snowflake session object
        table_name(str):
            Name of the table to create
        database (str):
            Name of the database
        schema (str):
            Name of the schema
    """
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {database}.{schema}.{table_name}(
        CODE VARCHAR,
        CODE_DESCRIPTION VARCHAR,
        VOCABULARY VARCHAR,
        CODELIST_ID VARCHAR,
        CODELIST_NAME VARCHAR,
        CODELIST_VERSION VARCHAR,
        DEFINITION_ID VARCHAR,
        DEFINITION_NAME VARCHAR,
        DEFINITION_VERSION VARCHAR,
        DEFINITION_SOURCE VARCHAR,
        VERSION_DATETIME TIMESTAMP_NTZ,
        UPLOADED_DATETIME TIMESTAMP_NTZ
    )
    """
    session.sql(create_table_sql).collect()
    print("Target table ensured")


def create_temp_definition_table(session: Session, df: pd.DataFrame, table_name: str,
        database: str = "INTELLIGENCE_DEV", schema: str = "AI_CENTRE_DEFINITION_LIBRARY"):
    """
    Creates a temporary table from a pandas DataFrame.
    The temporary table name will be prefixed with TEMP_.
    Args:
        session:
            Snowflake session object
        df (pd.DataFrame):
            Pandas df containing definition data
        table_name (str):
            Name of the target table (without TEMP_ prefix)
        database (str):
            Name of the database
        schema (str):
            Name of the schema
    """
    temp_table = f"TEMP_{table_name}"
    try:
        session.write_pandas(df, table_name=temp_table, overwrite=True, table_type="temporary",
            use_logical_type=True, database=database, schema=schema)
    except ProgrammingError:
        # Snowflake on streamlit does not allow temporary tables, so we use a normal table
        # There's a manual drop of this table after the merge
        session.write_pandas(df, table_name=temp_table, overwrite=True,
            use_logical_type=True, database=database, schema=schema)
    print(f"Loaded data to temporary table {database}.{schema}.{temp_table}")


def merge_definition_tables(
    session: Session,
    table_name: str,
    database: str = "INTELLIGENCE_DEV",
    schema: str = "AI_CENTRE_DEFINITION_LIBRARY"
):
    """
    Merges data from a source table into a target table.

    Args:
        session:
            Snowflake session object
        table_name (str):
            Name of the target table
    """
    merge_sql = f"""
    MERGE INTO {database}.{schema}.{table_name} target
    USING {database}.{schema}.TEMP_{table_name} source
    ON target.CODE = source.CODE
    AND target.CODELIST_VERSION = source.CODELIST_VERSION
    AND target.DEFINITION_NAME = source.DEFINITION_NAME
    AND target.DEFINITION_VERSION = source.DEFINITION_VERSION
    WHEN NOT MATCHED THEN
        INSERT (
            CODE, CODE_DESCRIPTION, VOCABULARY,
            CODELIST_ID, CODELIST_NAME, CODELIST_VERSION,
            DEFINITION_ID, DEFINITION_NAME, DEFINITION_VERSION,
            DEFINITION_SOURCE,
            VERSION_DATETIME, UPLOADED_DATETIME
        )
        VALUES (
            source.CODE, source.CODE_DESCRIPTION, source.VOCABULARY,
            source.CODELIST_ID, source.CODELIST_NAME, source.CODELIST_VERSION,
            source.DEFINITION_ID, source.DEFINITION_NAME, source.DEFINITION_VERSION,
            source.DEFINITION_SOURCE,
            source.VERSION_DATETIME, source.UPLOADED_DATETIME
        )
    """
    session.sql(merge_sql).collect()
    print("Merged data into main table")


def load_definitions_to_snowflake(session: Session, df: pd.DataFrame, table_name: str,
        database: str = "INTELLIGENCE_DEV", schema: str = "AI_CENTRE_DEFINITION_LIBRARY"):
    """
    Loads definition data to Snowflake using staging pattern.
    Args:
        session:
            Snowflake session
        df (pd.DataFrame):
            DataFrame containing definition data
        table_name (str):
            Name of target table
    """

    # Create permanent target table if not exists
    create_definition_table(session, table_name, database, schema)

    # Load to temp table
    create_temp_definition_table(session, df, table_name, database, schema)

    # Merge temp into permanent table
    merge_definition_tables(session, table_name, database, schema)

    session.sql(f"DROP TABLE IF EXISTS {database}.{schema}.TEMP_{table_name}").collect()
    print("Temporary table dropped")
