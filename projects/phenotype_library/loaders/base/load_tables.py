import pandas as pd


def create_phenotype_table(snowsesh, table_name: str):
    """
    Creates a phenotype table if it doesn't exist.
    Args:
        snowsesh:
            Snowflake session object
        table_name(str):
            Name of the table to create
    """
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name}(
        CODE VARCHAR,
        CODE_DESCRIPTION VARCHAR,
        VOCABULARY VARCHAR,
        CODELIST_ID VARCHAR,
        CODELIST_NAME VARCHAR,
        CODELIST_VERSION VARCHAR,
        PHENOTYPE_ID VARCHAR,
        PHENOTYPE_NAME VARCHAR,
        PHENOTYPE_VERSION VARCHAR,
        PHENOTYPE_SOURCE VARCHAR,
        VERSION_DATETIME TIMESTAMP_NTZ,
        UPLOADED_DATETIME TIMESTAMP_NTZ
    )
    """
    snowsesh.execute_query(create_table_sql)
    print("Target table ensured")


def create_temp_phenotype_table(snowsesh, df: pd.DataFrame, table_name: str):
    """
    Creates a temporary table from a pandas DataFrame.
    The temporary table name will be prefixed with TEMP_.
    Args:
        snowsesh:
            Snowflake session object
        df (pd.DataFrame):
            Pandas df containing phenotype data
        table_name (str):
            Name of the target table (without TEMP_ prefix)
    """
    temp_table = f"TEMP_{table_name}"

    snowsesh.load_dataframe_to_table(
        df=df, table_name=temp_table, mode="overwrite", table_type="temporary"
    )
    print("Loaded data to temporary table")


def merge_phenotype_tables(
    snowsesh,
    table_name: str,
):
    """
    Merges data from a source table into a target table.

    Args:
        snowsesh:
            Snowflake session object
        table_name (str):
            Name of the target table
    """
    merge_sql = f"""
    MERGE INTO {table_name} target
    USING TEMP_{table_name} source
    ON target.CODE = source.CODE
    AND target.CODE_DESCRIPTION = source.CODE_DESCRIPTION
    AND target.VOCABULARY = source.VOCABULARY
    AND target.CODELIST_VERSION = source.CODELIST_VERSION
    AND target.PHENOTYPE_NAME = source.PHENOTYPE_NAME
    AND target.PHENOTYPE_VERSION = source.PHENOTYPE_VERSION
    WHEN NOT MATCHED THEN
        INSERT (
            CODE, CODE_DESCRIPTION, VOCABULARY,
            CODELIST_ID, CODELIST_NAME, CODELIST_VERSION,
            PHENOTYPE_ID, PHENOTYPE_NAME, PHENOTYPE_VERSION,
            PHENOTYPE_SOURCE,
            VERSION_DATETIME, UPLOADED_DATETIME
        )
        VALUES (
            source.CODE, source.CODE_DESCRIPTION, source.VOCABULARY,
            source.CODELIST_ID, source.CODELIST_NAME, source.CODELIST_VERSION,
            source.PHENOTYPE_ID, source.PHENOTYPE_NAME, source.PHENOTYPE_VERSION,
            source.PHENOTYPE_SOURCE,
            source.VERSION_DATETIME, source.UPLOADED_DATETIME
        )
    """
    snowsesh.execute_query(merge_sql)
    print("Merged data into main table")


def load_phenotypes_to_snowflake(snowsesh, df: pd.DataFrame, table_name: str):
    """
    Loads phenotype data to Snowflake using staging pattern.
    Args:
        snowsesh:
            Snowflake session
        df (pd.DataFrame):
            DataFrame containing phenotype data
        table_name (str):
            Name of target table
    """
    try:
        # Create permanent target table if not exists
        create_phenotype_table(snowsesh, table_name)

        # Load to temp table
        create_temp_phenotype_table(snowsesh, df, table_name)

        # Merge temp into permanent table
        merge_phenotype_tables(snowsesh, table_name)

    except Exception as e:
        print(f"Error loading phenotypes: {e}")
        raise e
    finally:
        # Clean up
        try:
            snowsesh.execute_query(f"DROP TABLE IF EXISTS TEMP_{table_name}")
            print("Temporary table dropped")
        except Exception as e:
            print(f"Failed to clean up temporary table: {e}")
