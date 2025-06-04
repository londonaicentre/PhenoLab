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
    """
    try:
        with _snowsesh.use_context(database=SNOWFLAKE_DATABASE, schema=FEATURE_STORE):
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

        latest_table = measurement_tables.iloc[0]['TABLE_NAME']

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


@standard_query_cache
def get_condition_patient_counts_by_year(definition_name: str, _snowsesh: SnowflakeConnection) -> pd.DataFrame:
    """
    Get unique patient counts by year for a given condition definition
    Includes both SNOMED codes from OBSERVATION and ICD10/OPCS4 codes from BASE_APC_CONCEPTS

    Args:
        definition_name: Name of the condition definition
        _snowsesh: Snowflake connection

    Returns:
        DataFrame with columns: YEAR, PATIENT_COUNT
    """
    # Get latest BASE_APC_CONCEPTS table
    apc_table_query = f"""
    SELECT TABLE_NAME
    FROM {SNOWFLAKE_DATABASE}.INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = '{FEATURE_STORE}'
      AND TABLE_NAME LIKE 'BASE_APC_CONCEPTS%'
    ORDER BY TABLE_NAME DESC
    LIMIT 1
    """
    apc_result = _snowsesh.execute_query_to_df(apc_table_query)
    apc_table = apc_result.iloc[0]['TABLE_NAME'] if not apc_result.empty else None

    query_parts = []

    # SNOMED from OBSERVATION
    query_parts.append(f"""
    SELECT
        YEAR(obs.CLINICAL_EFFECTIVE_DATE) AS YEAR,
        obs.PERSON_ID
    FROM {DDS_OBSERVATION} obs
    LEFT JOIN {SNOWFLAKE_DATABASE}.{DEFINITION_LIBRARY}.DEFINITIONSTORE def
        ON obs.CORE_CONCEPT_ID = def.DBID
    WHERE def.DEFINITION_NAME = '{definition_name}'
        AND def.VOCABULARY = 'SNOMED'
        AND obs.CLINICAL_EFFECTIVE_DATE IS NOT NULL
    """)

    # ICD10/OPCS4 from BASE_APC_CONCEPTS
    if apc_table:
        query_parts.append(f"""
        SELECT
            YEAR(apc.ACTIVITY_DATE) AS YEAR,
            apc.PERSON_ID
        FROM {SNOWFLAKE_DATABASE}.{FEATURE_STORE}.{apc_table} apc
        INNER JOIN {SNOWFLAKE_DATABASE}.{DEFINITION_LIBRARY}.DEFINITIONSTORE def
            ON apc.VOCABULARY = def.VOCABULARY
            AND apc.CONCEPT_CODE_STD = def.CODE
        WHERE def.DEFINITION_NAME = '{definition_name}'
            AND def.VOCABULARY IN ('ICD10', 'OPCS4')
            AND apc.ACTIVITY_DATE IS NOT NULL
        """)

    # count patients per year
    combined_query = f"""
    WITH all_patients AS (
        {' UNION '.join(query_parts)}
    )
    SELECT
        YEAR,
        COUNT(DISTINCT PERSON_ID) AS PATIENT_COUNT
    FROM all_patients
    GROUP BY YEAR
    ORDER BY YEAR
    """

    return get_data_from_snowflake_to_dataframe(_snowsesh, combined_query)


@standard_query_cache
def get_unique_patients_for_condition(definition_name: str, _snowsesh: SnowflakeConnection) -> int:
    """
    Get total unique patient count for a condition definition
    Includes both SNOMED codes from OBSERVATION and ICD10/OPCS4 codes from BASE_APC_CONCEPTS

    Args:
        definition_name: Name of the condition definition
        _snowsesh: Snowflake connection

    Returns:
        Number of unique patients
    """
    # Get latest BASE_APC_CONCEPTS table
    apc_table_query = f"""
    SELECT TABLE_NAME
    FROM {SNOWFLAKE_DATABASE}.INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = '{FEATURE_STORE}'
      AND TABLE_NAME LIKE 'BASE_APC_CONCEPTS%'
    ORDER BY TABLE_NAME DESC
    LIMIT 1
    """
    apc_result = _snowsesh.execute_query_to_df(apc_table_query)
    apc_table = apc_result.iloc[0]['TABLE_NAME'] if not apc_result.empty else None

    # Build query with UNION for both sources
    query_parts = []

    # SNOMED from OBSERVATION
    query_parts.append(f"""
    SELECT DISTINCT obs.PERSON_ID
    FROM {DDS_OBSERVATION} obs
    LEFT JOIN {SNOWFLAKE_DATABASE}.{DEFINITION_LIBRARY}.DEFINITIONSTORE def
        ON obs.CORE_CONCEPT_ID = def.DBID
    WHERE def.DEFINITION_NAME = '{definition_name}'
        AND def.VOCABULARY = 'SNOMED'
    """)

    # ICD10/OPCS4 from BASE_APC_CONCEPTS
    if apc_table:
        query_parts.append(f"""
        SELECT DISTINCT apc.PERSON_ID
        FROM {SNOWFLAKE_DATABASE}.{FEATURE_STORE}.{apc_table} apc
        INNER JOIN {SNOWFLAKE_DATABASE}.{DEFINITION_LIBRARY}.DEFINITIONSTORE def
            ON apc.VOCABULARY = def.VOCABULARY
            AND apc.CONCEPT_CODE_STD = def.CODE
        WHERE def.DEFINITION_NAME = '{definition_name}'
            AND def.VOCABULARY IN ('ICD10', 'OPCS4')
        """)

    # count unique patients
    combined_query = f"""
    WITH all_patients AS (
        {' UNION '.join(query_parts)}
    )
    SELECT COUNT(DISTINCT PERSON_ID) AS UNIQUE_PATIENTS
    FROM all_patients
    """

    result = get_data_from_snowflake_to_dataframe(_snowsesh, combined_query)
    return result.iloc[0]['UNIQUE_PATIENTS'] if not result.empty else 0

