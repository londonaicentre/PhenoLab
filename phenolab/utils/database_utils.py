from typing import Optional

import pandas as pd
import streamlit as st

from snowflake.snowpark import Session
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.exceptions import SnowparkSessionException

"""
# database_utils.py

Snowflake connection and database query functions.

Uses a single snowflake connectino, and context managers for temporary db/schema switching.

Uses parameters in config.py to adapt to different source database naming.
"""

def get_snowflake_session() -> Session:
    try:
        return get_active_session() # this function works for Snowflake on Streamlit
    except SnowparkSessionException:
        # LOCAL DEVELOPMENT: load environment variables and use specified connection
        from dotenv import load_dotenv
        import os
        load_dotenv()

        connection_name = os.getenv("PHENOLAB_CONNECTION", "snowflake")

        # Default reads from connections.toml
        try:
            return Session.builder.config("connection_name", connection_name).create()
        except:
            # for backwards compatibility
            return st.connection(connection_name).session()


### DATABASE READS
def standard_query_cache(func):
    """
    Standard cache decorator for database queries
    """
    return st.cache_data(ttl=1800, show_spinner="Reading from database...")(func)


@standard_query_cache
def get_data_from_snowflake_to_dataframe(query: str) -> pd.DataFrame:
    return st.session_state.session.sql(query).to_pandas()


@standard_query_cache
def get_data_from_snowflake_to_list(query: str) -> list:
    return st.session_state.session.sql(query).collect()


@standard_query_cache
def get_definitions_from_snowflake_and_return_as_annotated_list_with_id_list() -> tuple[list, list]:

    comparison_query = f"""
    SELECT DISTINCT DEFINITION_SOURCE, DEFINITION_ID, DEFINITION_NAME
    FROM {st.session_state.config["definition_library"]["database"]}.
        {st.session_state.config["definition_library"]["schema"]}.DEFINITIONSTORE
    ORDER BY LOWER(DEFINITION_NAME)
    """
    comparison_definitions = get_data_from_snowflake_to_dataframe(comparison_query)

    return comparison_definitions["DEFINITION_ID"].to_list(), [
        f"{row['DEFINITION_NAME']} [{row['DEFINITION_SOURCE']}]"
        for _, row in comparison_definitions.iterrows()
    ]


@standard_query_cache
def return_codes_for_given_definition_id_as_df(chosen_definition_id: str) -> pd.DataFrame:
    codes_query = f"""
        SELECT DISTINCT
            CODE,
            CODE_DESCRIPTION,
            VOCABULARY,
            DEFINITION_ID,
            DEFINITION_NAME,
            DEFINITION_SOURCE,
            CODELIST_VERSION
        FROM {st.session_state.config["definition_library"]["database"]}.
            {st.session_state.config["definition_library"]["schema"]}.DEFINITIONSTORE
        WHERE DEFINITION_ID = '{chosen_definition_id}'
        ORDER BY VOCABULARY, CODE
        """
    return get_data_from_snowflake_to_dataframe(codes_query)


# @standard_query_cache
# def get_aic_definitions() -> pd.DataFrame:
#     """
#     Get all AI Centre definitions with metadata
#     """
#     query = f"""
#     SELECT DEFINITION_ID, DEFINITION_NAME,
#         VERSION_DATETIME, UPLOADED_DATETIME
#     FROM {st.session_state.config["definition_library"]["database"]}.
#         {st.session_state.config["definition_library"]["schema"]}.AIC_DEFINITIONS
#     GROUP BY DEFINITION_ID, DEFINITION_NAME, VERSION_DATETIME, UPLOADED_DATETIME
#     ORDER BY DEFINITION_NAME
#     """
#     return get_data_from_snowflake_to_dataframe(query)


@standard_query_cache
def get_measurement_unit_statistics(definition_name: str) -> pd.DataFrame:
    """
    Get statistics for all units associated with a measurement definition
    """
    query = f"""
    WITH measurement_values AS (
        SELECT
            obs.RESULT_VALUE_UNIT,
            TRY_CAST(obs.RESULT_VALUE AS FLOAT) AS VALUE
        FROM {st.session_state.config["gp_observation_table"]} obs
        INNER JOIN {st.session_state.config["definition_library"]["database"]}.
            {st.session_state.config["definition_library"]["schema"]}.DEFINITIONSTORE def
            ON obs.OBSERVATION_CONCEPT_CODE = def.CODE
            AND obs.OBSERVATION_CONCEPT_VOCABULARY = def.VOCABULARY
        WHERE def.DEFINITION_NAME = '{definition_name}'
            AND obs.RESULT_VALUE IS NOT NULL
    )
    SELECT
        COALESCE(RESULT_VALUE_UNIT, 'No Unit') AS UNIT,
        COUNT(*) AS TOTAL_COUNT,
        COUNT(VALUE) AS NUMERIC_COUNT,
        APPROX_PERCENTILE(VALUE, 0.25) AS LOWER_QUARTILE,
        APPROX_PERCENTILE(VALUE, 0.5) AS MEDIAN,
        APPROX_PERCENTILE(VALUE, 0.75) AS UPPER_QUARTILE,
        MIN(VALUE) AS MIN_VALUE,
        MAX(VALUE) AS MAX_VALUE
    FROM measurement_values
    GROUP BY RESULT_VALUE_UNIT
    ORDER BY TOTAL_COUNT DESC
    """
    print(query)
    return get_data_from_snowflake_to_dataframe(query)


def get_available_measurements() -> pd.DataFrame:
    """
    Get available measurement definitions from DEV_MEASUREMENTS tables in feature store
    """
    tables_query = f"""
    SELECT TABLE_NAME
    FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = '{st.session_state.config["feature_store"]["schema"]}'
        AND TABLE_NAME LIKE 'DEV_MEASUREMENTS%'
    ORDER BY TABLE_NAME DESC
    """
    measurement_tables = get_data_from_snowflake_to_dataframe(tables_query)

    if measurement_tables.empty:
        return pd.DataFrame()

    latest_table = measurement_tables.iloc[0]['TABLE_NAME']

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
    measurement_features = get_data_from_snowflake_to_dataframe(definitions_query)

    return measurement_features


@standard_query_cache
def get_condition_patient_counts_by_year(definition_name: str) -> pd.DataFrame:
    """
    Get unique patient counts by year for a given condition definition
    Includes both SNOMED codes from OBSERVATION and ICD10/OPCS4 codes from BASE_APC_CONCEPTS

    Args:
        definition_name: Name of the condition definition
        _session: Snowflake connection

    Returns:
        DataFrame with columns: YEAR, PATIENT_COUNT
    """
    query_parts = []

    # SNOMED from OBSERVATION
    query_parts.append(f"""
    SELECT
        YEAR(obs.CLINICAL_EFFECTIVE_DATE) AS YEAR,
        obs.PERSON_ID
    FROM {st.session_state.config["gp_observation_table"]} obs
    INNER JOIN {st.session_state.config["definition_library"]["database"]}.
            {st.session_state.config["definition_library"]["schema"]}.DEFINITIONSTORE def
        ON obs.OBSERVATION_CONCEPT_CODE = def.CODE
        AND obs.OBSERVATION_CONCEPT_VOCABULARY = def.VOCABULARY
    WHERE def.DEFINITION_NAME = '{definition_name}'
        AND def.VOCABULARY = 'SNOMED'
        AND obs.CLINICAL_EFFECTIVE_DATE IS NOT NULL
        AND YEAR(obs.CLINICAL_EFFECTIVE_DATE) BETWEEN 2000 AND YEAR(CURRENT_DATE())
    """)

    # ICD10 from STG_SUS__APC_DIAGNOSIS_ICD10
    query_parts.append(f"""
    SELECT
        YEAR(icd.ACTIVITY_DATE) AS YEAR,
        icd.PERSON_ID
    FROM {st.session_state.config["sus_icd10_table"]} icd
    INNER JOIN {st.session_state.config["definition_library"]["database"]}.
        {st.session_state.config["definition_library"]["schema"]}.DEFINITIONSTORE def
        ON icd.CONCEPT_CODE = def.CODE
        AND def.VOCABULARY = 'ICD10'
    WHERE def.DEFINITION_NAME = '{definition_name}'
        AND icd.ACTIVITY_DATE IS NOT NULL
        AND YEAR(icd.ACTIVITY_DATE) BETWEEN 2000 AND YEAR(CURRENT_DATE())
    """)

    # OPCS4 from STG_SUS__APC_PROCEDURE_OPCS4
    query_parts.append(f"""
    SELECT
        YEAR(opcs.ACTIVITY_DATE) AS YEAR,
        opcs.PERSON_ID
    FROM {st.session_state.config["sus_opcs4_table"]} opcs
    INNER JOIN {st.session_state.config["definition_library"]["database"]}.
        {st.session_state.config["definition_library"]["schema"]}.DEFINITIONSTORE def
        ON opcs.CONCEPT_CODE = def.CODE
        AND def.VOCABULARY = 'OPCS4'
    WHERE def.DEFINITION_NAME = '{definition_name}'
        AND opcs.ACTIVITY_DATE IS NOT NULL
        AND YEAR(opcs.ACTIVITY_DATE) BETWEEN 2000 AND YEAR(CURRENT_DATE())
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

    return get_data_from_snowflake_to_dataframe(combined_query)


@standard_query_cache
def get_unique_patients_for_condition(definition_name: str) -> int:
    """
    Get total unique patient count for a condition definition
    Includes SNOMED codes from OBSERVATION, ICD10 codes from STG_SUS__APC_DIAGNOSIS_ICD10,
    and OPCS4 codes from STG_SUS__APC_PROCEDURE_OPCS4

    Args:
        definition_name: Name of the condition definition
        _session: Snowflake connection

    Returns:
        Number of unique patients
    """
    # Build query with UNION for all sources
    query_parts = []

    # SNOMED from OBSERVATION
    query_parts.append(f"""
    SELECT DISTINCT obs.PERSON_ID
    FROM {st.session_state.config["gp_observation_table"]} obs
    INNER JOIN {st.session_state.config["definition_library"]["database"]}.
        {st.session_state.config["definition_library"]["schema"]}.DEFINITIONSTORE def
        ON obs.OBSERVATION_CONCEPT_CODE = def.CODE
        AND obs.OBSERVATION_CONCEPT_VOCABULARY = def.VOCABULARY
    WHERE def.DEFINITION_NAME = '{definition_name}'
        AND def.VOCABULARY = 'SNOMED'
        AND YEAR(obs.CLINICAL_EFFECTIVE_DATE) BETWEEN 2000 AND YEAR(CURRENT_DATE())
    """)

    # ICD10 from STG_SUS__APC_DIAGNOSIS_ICD10
    query_parts.append(f"""
    SELECT DISTINCT icd.PERSON_ID
    FROM {st.session_state.config["sus_icd10_table"]} icd
    INNER JOIN {st.session_state.config["definition_library"]["database"]}.
        {st.session_state.config["definition_library"]["schema"]}.DEFINITIONSTORE def
        ON icd.CONCEPT_CODE = def.CODE
        AND def.VOCABULARY = 'ICD10'
    WHERE def.DEFINITION_NAME = '{definition_name}'
        AND YEAR(icd.ACTIVITY_DATE) BETWEEN 2000 AND YEAR(CURRENT_DATE())
    """)

    # OPCS4 from STG_SUS__APC_PROCEDURE_OPCS4
    query_parts.append(f"""
    SELECT DISTINCT opcs.PERSON_ID
    FROM {st.session_state.config["sus_opcs4_table"]} opcs
    INNER JOIN {st.session_state.config["definition_library"]["database"]}.
        {st.session_state.config["definition_library"]["schema"]}.DEFINITIONSTORE def
        ON opcs.CONCEPT_CODE = def.CODE
        AND def.VOCABULARY = 'OPCS4'
    WHERE def.DEFINITION_NAME = '{definition_name}'
        AND YEAR(opcs.ACTIVITY_DATE) BETWEEN 2000 AND YEAR(CURRENT_DATE())
    """)

    # count unique patients
    combined_query = f"""
    WITH all_patients AS (
        {' UNION '.join(query_parts)}
    )
    SELECT COUNT(DISTINCT PERSON_ID) AS UNIQUE_PATIENTS
    FROM all_patients
    """

    result = get_data_from_snowflake_to_dataframe(combined_query)
    return result.iloc[0]['UNIQUE_PATIENTS'] if not result.empty else 0