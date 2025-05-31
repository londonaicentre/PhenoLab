import pandas as pd
import streamlit as st

#from dotenv import load_dotenv
from phmlondon.config import DDS_OBSERVATION, DEFINITION_LIBRARY, SNOWFLAKE_DATABASE, FEATURE_STORE
from phmlondon.snow_utils import SnowflakeConnection

"""
# database_utils.py

Snowflake connection and database query functions.

Uses a single snowflake connectino, and context managers for temporary db/schema switching.

Uses parameters in config.py to adapt to different source database naming.
"""

### SNOWFLAKE CONNECTION
def get_snowflake_connection() -> SnowflakeConnection:
    """
    Get or create the single Snowflake connection for the session.

    This creates one connection per Streamlit session and stores it in session state.
    The connection can be used with different databases/schemas using the context manager:

    E.g.:
        snowsesh = get_snowflake_connection()

        df = snowsesh.execute_query_to_df("SELECT * FROM my_table")

        # use with different database/schema
        with snowsesh.use_context(database="PROD_DWH", schema="ANALYST_PRIMARY_CARE"):
            df2 = snowsesh.execute_query_to_df("SELECT * FROM other_table")
    """
    if "snowflake_connection" not in st.session_state:
        with st.spinner("Connecting to Snowflake..."):
            try:
                conn = SnowflakeConnection()

                # default db and schema
                conn.use_database(SNOWFLAKE_DATABASE)
                conn.use_schema(DEFINITION_LIBRARY)
                st.session_state.snowflake_connection = conn
            except Exception as e:
                st.error(f"Failed to connect to Snowflake: {e}")
                raise

    return st.session_state.snowflake_connection

# BACKWARDS COMPATIBILITY
connect_to_snowflake = get_snowflake_connection

### DATABASE READS
def standard_query_cache(func):
    """
    Standard cache decorator for database queries
    """
    return st.cache_data(ttl=1800, show_spinner="Reading from database...")(func)


@standard_query_cache
def get_data_from_snowflake_to_dataframe(_snowsesh: SnowflakeConnection, query: str) -> pd.DataFrame:
    return _snowsesh.execute_query_to_df(query)


@standard_query_cache
def get_data_from_snowflake_to_list(_snowsesh: SnowflakeConnection, query: str) -> list:
    return _snowsesh.session.sql(query).collect()


@standard_query_cache
def get_definitions_from_snowflake_and_return_as_annotated_list_with_id_list(
    _snowsesh: SnowflakeConnection,
) -> tuple[list, list]:
    comparison_query = f"""
    SELECT DISTINCT DEFINITION_SOURCE, DEFINITION_ID, DEFINITION_NAME
    FROM {SNOWFLAKE_DATABASE}.{DEFINITION_LIBRARY}.DEFINITIONSTORE
    ORDER BY DEFINITION_NAME
    """
    # comparison_defintions = _snowsesh.execute_query_to_df(comparison_query)
    comparison_definitions = get_data_from_snowflake_to_dataframe(_snowsesh, comparison_query)

    return comparison_definitions["DEFINITION_ID"].to_list(), [
        f"[{row['DEFINITION_SOURCE']}] [{row['DEFINITION_ID']}] {row['DEFINITION_NAME']}"
        for _, row in comparison_definitions.iterrows()
    ]


@standard_query_cache
def return_codes_for_given_definition_id_as_df(
    _snowsesh: SnowflakeConnection, chosen_definition_id: str
) -> pd.DataFrame:
    codes_query = f"""
        SELECT DISTINCT
            CODE,
            CODE_DESCRIPTION,
            VOCABULARY,
            DEFINITION_ID,
            CODELIST_VERSION
        FROM {SNOWFLAKE_DATABASE}.{DEFINITION_LIBRARY}.DEFINITIONSTORE
        WHERE DEFINITION_ID = '{chosen_definition_id}'
        ORDER BY VOCABULARY, CODE
        """
    return get_data_from_snowflake_to_dataframe(_snowsesh, codes_query)


@standard_query_cache
def get_aic_definitions(_snowsesh: SnowflakeConnection) -> pd.DataFrame:
    """
    Get all AI Centre definitions with metadata
    """
    query = f"""
    SELECT DEFINITION_ID, DEFINITION_NAME,
        VERSION_DATETIME, UPLOADED_DATETIME
    FROM {DEFINITION_LIBRARY}.AIC_DEFINITIONS
    GROUP BY DEFINITION_ID, DEFINITION_NAME, VERSION_DATETIME, UPLOADED_DATETIME
    ORDER BY DEFINITION_NAME
    """
    return get_data_from_snowflake_to_dataframe(_snowsesh, query)


@standard_query_cache
def get_measurement_unit_statistics(definition_name: str, _snowsesh: SnowflakeConnection) -> pd.DataFrame:
    """
    Get statistics for all units associated with a measurement definition
    """
    query = f"""
    SELECT DISTINCT
        RESULT_VALUE_UNITS AS unit,
        COUNT(*) AS total_count,
        COUNT_IF(TRY_CAST(RESULT_VALUE AS FLOAT) IS NOT NULL) AS numeric_count,
        APPROX_PERCENTILE(TRY_CAST(RESULT_VALUE AS FLOAT), 0.25) AS lower_quartile,
        APPROX_PERCENTILE(TRY_CAST(RESULT_VALUE AS FLOAT), 0.5) AS median,
        APPROX_PERCENTILE(TRY_CAST(RESULT_VALUE AS FLOAT), 0.75) AS upper_quartile,
        MIN(TRY_CAST(RESULT_VALUE AS FLOAT)) AS min_value,
        MAX(TRY_CAST(RESULT_VALUE AS FLOAT)) AS max_value
    FROM {DDS_OBSERVATION} obs
    LEFT JOIN {SNOWFLAKE_DATABASE}.{DEFINITION_LIBRARY}.DEFINITIONSTORE def
        ON obs.CORE_CONCEPT_ID = def.DBID
    WHERE RESULT_VALUE_UNITS IS NOT NULL
    AND def.DEFINITION_NAME = '{definition_name}'
    GROUP BY RESULT_VALUE_UNITS
    ORDER BY total_count DESC
    """
    print(query)
    return _snowsesh.execute_query_to_df(query)


def get_available_measurements(_snowsesh):
    """
    Get available measurement definitions from BASE_MEASUREMENTS tables in feature store
    
    Returns:
        pandas.DataFrame: Available measurement definitions with columns:
        - DEFINITION_ID, DEFINITION_NAME, VALUE_UNITS, MEASUREMENT_COUNT, TABLE_NAME
    """
    try:
        with _snowsesh.use_context(database=SNOWFLAKE_DATABASE, schema=FEATURE_STORE):
            # Get all BASE_MEASUREMENTS tables
            tables_query = f"""
            SELECT TABLE_NAME 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = '{FEATURE_STORE}' 
              AND TABLE_NAME LIKE 'BASE_MEASUREMENTS%'
            ORDER BY TABLE_NAME DESC
            """
            measurement_tables = _snowsesh.execute_query_to_df(tables_query)
            
        if measurement_tables.empty:
            return pd.DataFrame()
            
        # Use the latest version (highest version number)
        latest_table = measurement_tables.iloc[0]['TABLE_NAME']
        
        # Get available measurement definitions from the table
        with _snowsesh.use_context(database=SNOWFLAKE_DATABASE, schema=FEATURE_STORE):
            definitions_query = f"""
            SELECT DISTINCT 
                DEFINITION_ID,
                DEFINITION_NAME,
                VALUE_UNITS,
                COUNT(*) as MEASUREMENT_COUNT,
                '{latest_table}' as TABLE_NAME
            FROM {latest_table}
            GROUP BY DEFINITION_ID, DEFINITION_NAME, VALUE_UNITS
            ORDER BY DEFINITION_NAME
            """
            measurement_features = _snowsesh.execute_query_to_df(definitions_query)
            
        return measurement_features
        
    except Exception as e:
        st.error(f"Error loading measurement features: {e}")
        return pd.DataFrame()

