import os
import re

import pandas as pd
import streamlit as st
from dotenv import load_dotenv

from phmlondon.snow_utils import SnowflakeConnection


@st.cache_resource(show_spinner="Connecting to Snowflake...")
def connect_to_snowflake() -> SnowflakeConnection:
    load_dotenv()

    if "snowsesh" not in st.session_state:
        st.session_state.snowsesh = SnowflakeConnection()
        st.session_state.snowsesh.use_database("INTELLIGENCE_DEV")
        st.session_state.snowsesh.use_schema("AI_CENTRE_DEFINITION_LIBRARY")

    snowsesh = st.session_state.snowsesh
    return snowsesh


@st.cache_data(show_spinner="Reading from database...")
def get_data_from_snowflake_to_dataframe(_session: SnowflakeConnection, query: str) -> pd.DataFrame:
    return _session.execute_query_to_df(query)


@st.cache_data
def get_data_from_snowflake_to_list(_session: SnowflakeConnection, query: str) -> list:
    return _session.session.sql(query).collect()


@st.cache_data
def get_definitions_from_snowflake_and_return_as_annotated_list_with_id_list(
    _session: SnowflakeConnection,
) -> tuple[list, list]:
    comparison_query = """
    SELECT DISTINCT DEFINITION_SOURCE, DEFINITION_ID, DEFINITION_NAME
    FROM INTELLIGENCE_DEV.AI_CENTRE_DEFINITION_LIBRARY.DEFINITIONSTORE
    ORDER BY DEFINITION_NAME
    """
    # comparison_defintions = snowsesh.execute_query_to_df(comparison_query)
    comparison_definitions = get_data_from_snowflake_to_dataframe(_session, comparison_query)

    return comparison_definitions["DEFINITION_ID"].to_list(), [
        f"[{row['DEFINITION_SOURCE']}] [{row['DEFINITION_ID']}] {row['DEFINITION_NAME']}"
        for i, row in comparison_definitions.iterrows()
    ]


@st.cache_data(show_spinner="Searching for codes for this definition...")
def return_codes_for_given_definition_id_as_df(_conn: SnowflakeConnection, chosen_definition_id: str) -> pd.DataFrame:
    codes_query = f"""
        SELECT DISTINCT
            CODE,
            CODE_DESCRIPTION,
            VOCABULARY,
            DEFINITION_ID,
            CODELIST_VERSION
        FROM INTELLIGENCE_DEV.AI_CENTRE_DEFINITION_LIBRARY.DEFINITIONSTORE
        WHERE DEFINITION_ID = '{chosen_definition_id}'
        ORDER BY VOCABULARY, CODE
        """
    return get_data_from_snowflake_to_dataframe(_conn, codes_query)

@st.cache_data
def get_definition_sources(_connection: SnowflakeConnection) -> list:
    def_source_query =  """SELECT DISTINCT DEFINITION_SOURCE
                        FROM intelligence_dev.ai_centre_definition_library.definitionstore
                        """
    def_source_df = _connection.execute_query_to_df(def_source_query)
    return def_source_df.iloc[:, 0].to_list()

@st.cache_data
def get_definitions(source: str, _connection: SnowflakeConnection) -> list:
    def_query = f"""SELECT DISTINCT DEFINITION_NAME
                FROM intelligence_dev.ai_centre_definition_library.definitionstore
                WHERE code is not null
                and DEFINITION_SOURCE = '{source}'
                """
    def_df = _connection.execute_query_to_df(def_query)
    return def_df.iloc[:, 0].to_list()

@st.cache_data
def get_existing_units(definition: str, _connection: SnowflakeConnection) -> list:
    unit_query = f"""SELECT DISTINCT CLEANED_UNITS
    FROM INTELLIGENCE_DEV.AI_CENTRE_OBSERVATION_STAGING_TABLES.UNIT_LOOKUP
    WHERE DEFINITION_NAME = '{definition}'
    """
    return _connection.execute_query_to_list(unit_query)

@st.cache_data
def get_existing_units_from_cleaned(units: str, definition: str, _connection: SnowflakeConnection) -> list:
    unit_query = f"""SELECT DISTINCT RESULT_VALUE_UNITS
    FROM INTELLIGENCE_DEV.AI_CENTRE_OBSERVATION_STAGING_TABLES.UNIT_LOOKUP
    WHERE DEFINITION_NAME = '{definition}'
    AND CLEANED_UNITS = '{units}'
    """
    return _connection.execute_query_to_list(unit_query)

@st.cache_data
def get_unit_info(definition: str, _connection: SnowflakeConnection) -> pd.DataFrame:
    info_query = f"""SELECT DISTINCT RESULT_VALUE_UNITS,
    COUNT(*) count_units,
    AVG(result_value) mean,
    median(result_value) med,
    min(result_value) minimum_value,
    max(result_value) maximum_value
    FROM PROD_DWH.ANALYST_PRIMARY_CARE.OBSERVATION obs
    left join intelligence_dev.ai_centre_definition_library.definitionstore def on obs.core_concept_id = def.dbid
    where result_value is not null
    and result_value_units is not null
    and def.DEFINITION_NAME ='{definition}'
    GROUP BY RESULT_VALUE_UNITS
    ORDER BY count_units desc"""
    return _connection.execute_query_to_df(info_query)

@st.cache_data(show_spinner="Extracting distribution data")
def get_unit_distributions(definition: str,
                            units: list[str],
                            _connection: SnowflakeConnection) -> pd.DataFrame:
    unit_distr_query = f"""SELECT RESULT_VALUE,
    RESULT_VALUE_UNITS
    FROM PROD_DWH.ANALYST_PRIMARY_CARE.OBSERVATION obs
    left join intelligence_dev.ai_centre_definition_library.definitionstore def on obs.core_concept_id = def.dbid
    where result_value is not null
    and result_value_units is not null
    and def.DEFINITION_NAME ='{definition}'
    and obs.result_value_units in ({', '.join('\'' + unit + '\'' for unit in units)})
    LIMIT 1000000"""
    return _connection.execute_query_to_df(unit_distr_query)

def get_sql_files(directory = None) -> list:
    """Returns all the sql files in the directory"""
    files =  os.listdir(directory)
    return [file for file in files if re.findall('.*\\.sql', file)]