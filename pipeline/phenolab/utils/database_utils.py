import pandas as pd
import streamlit as st
from dotenv import load_dotenv

from phmlondon.snow_utils import SnowflakeConnection
from phmlondon.config import SNOWFLAKE_DATABASE, DEFINITION_LIBRARY


@st.cache_resource(show_spinner="Connecting to Snowflake...")
def connect_to_snowflake() -> SnowflakeConnection:
    """
    Creates a cached connection to Snowflake using config variables.
    Returns:
        SnowflakeConnection: Active connection to Snowflake
    """

    if "snowsesh" not in st.session_state:
        st.session_state.snowsesh = SnowflakeConnection()
        st.session_state.snowsesh.use_database(SNOWFLAKE_DATABASE)
        st.session_state.snowsesh.use_schema(DEFINITION_LIBRARY)

    snowsesh = st.session_state.snowsesh
    return snowsesh


@st.cache_data(show_spinner="Reading from database...")
def get_data_from_snowflake_to_dataframe(_snowsesh: SnowflakeConnection, query: str) -> pd.DataFrame:
    return _snowsesh.execute_query_to_df(query)


@st.cache_data
def get_data_from_snowflake_to_list(_snowsesh: SnowflakeConnection, query: str) -> list:
    return _snowsesh.session.sql(query).collect()


@st.cache_data
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
        for i, row in comparison_definitions.iterrows()
    ]


@st.cache_data(show_spinner="Searching for codes for this definition...")
def return_codes_for_given_definition_id_as_df(_snowsesh: SnowflakeConnection, chosen_definition_id: str) -> pd.DataFrame:
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


@st.cache_data
def get_measurement_unit_statistics(definition_name: str, _snowsesh: SnowflakeConnection) -> pd.DataFrame:
    """
    Get statistics for all units associated with a measurement definition
    """
    query = f"""
    SELECT DISTINCT
        RESULT_VALUE_UNITS AS unit,
        COUNT(*) AS count,
        APPROX_PERCENTILE(RESULT_VALUE, 0.25) AS lower_quartile,
        APPROX_PERCENTILE(RESULT_VALUE, 0.5) AS median,
        APPROX_PERCENTILE(RESULT_VALUE, 0.75) AS upper_quartile,
        MIN(RESULT_VALUE) AS min_value,
        MAX(RESULT_VALUE) AS max_value
    FROM PROD_DWH.ANALYST_PRIMARY_CARE.OBSERVATION obs
    LEFT JOIN {SNOWFLAKE_DATABASE}.{DEFINITION_LIBRARY}.DEFINITIONSTORE def
        ON obs.CORE_CONCEPT_ID = def.DBID
    WHERE RESULT_VALUE_UNITS IS NOT NULL
    AND def.DEFINITION_NAME = '{definition_name}'
    GROUP BY RESULT_VALUE_UNITS
    ORDER BY count DESC
    """
    return _snowsesh.execute_query_to_df(query)