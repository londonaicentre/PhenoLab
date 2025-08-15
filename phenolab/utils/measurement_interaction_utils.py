import os
from decimal import Decimal
from typing import Optional, List

import pandas as pd
import streamlit as st
from snowflake.snowpark import Session
from utils.database_utils import get_measurement_unit_statistics
from utils.definition_interaction_utils import load_definition
from utils.measurement import MeasurementConfig, UnitMapping, load_measurement_config_from_json


def load_measurement_definitions_list() -> list[str]:
    """
    Get list of measurement definition files from /data/definitions
    """
    definitions_list = []
    try:
        if os.path.exists("data/definitions"):
            definitions_list = [f for f in os.listdir("data/definitions")
                               if f.endswith(".json") and "measurement_" in f]
    except Exception as e:
        st.error(f"Unable to list measurement definition files: {e}")
    return definitions_list


def load_measurement_configs_list(config: Optional[dict] = None) -> List[str]:
    """
    Get list of measurement config files from /data/measurements (shared across all ICBs)

    Args:
        config (Optional[dict]): Configuration dictionary. If not provided, will use session state.
    """
    measurement_config_list = []
    config = config or st.session_state.config
    print(f"Loading shared measurement configs for ICB: {config['icb_name']}")
    if os.path.exists("data/measurements"):
        measurement_config_list = [f for f in os.listdir("data/measurements")
                        if f.endswith(".json") and f.startswith("standard_")]
    return measurement_config_list


def load_measurement_config(filename: str, config: Optional[dict] = None) -> Optional[MeasurementConfig]:
    """
    Load measurement config from JSON file (shared across all ICBs)

    Args:
        filename (str): Name of the measurement config file
        config (Optional[dict]): Configuration dictionary. If not provided, will use session state.
    """
    config = config or st.session_state.config
    file_path = os.path.join("data/measurements", filename)
    measurement_config = load_measurement_config_from_json(file_path)
    return measurement_config


def create_missing_measurement_configs():
    """
    Create empty measurement configs for definitions that don't have one
    """
    os.makedirs("data/measurements", exist_ok=True)

    measurement_definitions = load_measurement_definitions_list()
    measurement_configs = load_measurement_configs_list()

    config_definition_ids = set()
    for config_file in measurement_configs:
        try:
            config = load_measurement_config(config_file)
            if config:
                config_definition_ids.add(config.definition_id)
        except Exception as e:
            st.warning(f"Could not load configuration file {config_file}: {e}")

    created_count = 0
    for def_file in measurement_definitions:
        try:
            file_path = os.path.join("data/definitions", def_file)
            definition = load_definition(file_path)

            if definition and definition.definition_id not in config_definition_ids:
                config = MeasurementConfig(
                    definition_id=definition.definition_id,
                    definition_name=definition.definition_name,
                    standard_measurement_config_id=None,
                    standard_measurement_config_version=None,
                )
                config.save_to_json(directory="data/measurements")
                created_count += 1

        except Exception as e:
            st.warning(f"Could not process {def_file}: {e}")

    return created_count


def update_all_measurement_configs():
    """
    Update all measurement configs with new units from Snowflake usage data
    """
    os.makedirs("data/measurements", exist_ok=True)

    created_count = 0
    updated_count = 0
    new_units_count = 0

    missing_created = create_missing_measurement_configs()
    created_count += missing_created

    existing_configs = {}
    for config_file in load_measurement_configs_list():
        try:
            config = load_measurement_config(config_file)
            if config:
                existing_configs[config.definition_name] = config
        except Exception as e:
            st.warning(f"Could not load config {config_file}: {e}")

    for def_name, config in existing_configs.items():
        # try:
        unit_stats = get_measurement_unit_statistics(def_name)

        if unit_stats is None or unit_stats.empty:
            continue

        existing_source_units = {m.source_unit for m in config.unit_mappings}
        config_changed = False

        for idx, row in unit_stats.iterrows():
            source_unit = row['UNIT']

            if source_unit not in existing_source_units:
                config.unit_mappings.append(UnitMapping(
                    source_unit=source_unit,
                    standard_unit="",
                    source_unit_count=row['TOTAL_COUNT'],
                    source_unit_lq=row['LOWER_QUARTILE'],
                    source_unit_median=row['MEDIAN'],
                    source_unit_uq=row['UPPER_QUARTILE']
                ))
                new_units_count += 1
                config_changed = True

        if config_changed:
            config.mark_modified()
            config.save_to_json(directory="data/measurements")
            updated_count += 1

        # except Exception as e:
        #     st.warning(f"Error processing {def_name}: {e}")

    return created_count, updated_count, new_units_count


def get_available_measurement_configs():
    """
    Get measurement configs that have standard units and mappings configured
    """
    measurement_configs = load_measurement_configs_list()
    available_configs = {}

    for config_file in measurement_configs:
        try:
            config = load_measurement_config(config_file)
            if (config and
                config.standard_units and
                config.primary_standard_unit):
                available_configs[config.definition_name] = config
        except Exception as e:
            st.warning(f"Error loading config {config_file}: {e}")
            continue

    return available_configs


@st.cache_data(ttl=600, show_spinner="Loading measurement values...")
def get_measurement_values(definition_name, limit = 100000):
    """
    Get actual measurement values for a definition from Snowflake using INT_OBSERVATION table
    """
    query = f"""
    SELECT
        COALESCE(RESULT_VALUE_UNIT, 'No Unit') AS unit,
        TRY_CAST(RESULT_VALUE AS FLOAT) AS value
    FROM {st.session_state.config["int_observation_table"]} obs
    INNER JOIN {st.session_state.config["definition_library"]["database"]}.
        {st.session_state.config["definition_library"]["schema"]}.DEFINITIONSTORE def
        ON obs.OBSERVATION_CONCEPT_CODE = def.CODE
        AND obs.OBSERVATION_CONCEPT_VOCABULARY = def.VOCABULARY
    WHERE def.DEFINITION_NAME = '{definition_name}'
        AND RESULT_VALUE IS NOT NULL
        AND TRY_CAST(RESULT_VALUE AS FLOAT) IS NOT NULL
    LIMIT {limit}
    """
    df = st.session_state.session.sql(query).to_pandas()
    df.columns = df.columns.str.lower()
    return df


def apply_unit_mapping(df, config):
    """
    Apply unit mappings to convert source units to standard units
    """
    df_mapped = df.copy()
    unit_mapping_dict = {m.source_unit: m.standard_unit for m in config.unit_mappings if m.standard_unit}
    df_mapped['mapped_unit'] = df_mapped['unit'].map(unit_mapping_dict)
    return df_mapped


def apply_conversions(df, config):
    """
    Apply unit conversions to convert values to primary standard unit
    """
    if not config.primary_standard_unit:
        return pd.DataFrame()

    df_converted = df.copy()
    df_converted['converted_value'] = df_converted['value']
    df_converted['converted_unit'] = config.primary_standard_unit

    conversion_dict = {}
    for conv in config.unit_conversions:
        if conv.convert_to_unit == config.primary_standard_unit:
            conversion_dict[conv.convert_from_unit] = conv

    for idx, row in df_converted.iterrows():
        unit_to_convert = row['mapped_unit'] if pd.notna(row['mapped_unit']) else row['unit']

        if unit_to_convert in conversion_dict:
            conv = conversion_dict[unit_to_convert]
            original_value = row['value']
            converted_value = (original_value + conv.pre_offset) * conv.multiply_by + conv.post_offset
            df_converted.at[idx, 'converted_value'] = converted_value
        elif unit_to_convert == config.primary_standard_unit:
            df_converted.at[idx, 'converted_value'] = row['value']

    return df_converted


def create_base_measurements_sql(eligible_configs):
    """
    Generate dynamic SQL query for Base Measurements feature table
    """
    union_queries = []

    for definition_name, config in eligible_configs.items():
        unit_mappings = {m.source_unit: m.standard_unit for m in config.unit_mappings if m.standard_unit}

        mapped_standard_units = set(unit_mappings.values())

        conversion_cases = []

        explicit_conversions = set()
        for conv in config.unit_conversions:
            if conv.convert_to_unit == config.primary_standard_unit:
                conversion_cases.append(f"""
                    WHEN mapped_unit = '{conv.convert_from_unit}' THEN
                        (({conv.pre_offset} + TRY_CAST(obs.RESULT_VALUE AS FLOAT)) * {conv.multiply_by}) + {conv.post_offset}
                """)
                explicit_conversions.add(conv.convert_from_unit)

        for standard_unit in mapped_standard_units:
            if standard_unit not in explicit_conversions:
                conversion_cases.append(f"""
                    WHEN mapped_unit = '{standard_unit}' THEN
                        TRY_CAST(obs.RESULT_VALUE AS FLOAT)
                """)

        mapping_cases = []
        for source_unit, standard_unit in unit_mappings.items():
            if source_unit == 'No Unit':
                mapping_cases.append(f"WHEN obs.RESULT_VALUE_UNIT IS NULL THEN '{standard_unit}'")
            else:
                mapping_cases.append(f"WHEN obs.RESULT_VALUE_UNIT = '{source_unit}' THEN '{standard_unit}'")

        # Handle unitless measurements (like indices) that don't need mappings
        if not mapping_cases:
            # For unitless measurements, use the primary standard unit directly
            mapping_case_sql = f"'{config.primary_standard_unit}'"
        else:
            mapping_case_sql = f"""
                CASE
                    {' '.join(mapping_cases)}
                    ELSE NULL
                END
            """

        # Handle unitless measurements that don't need conversions
        if not conversion_cases:
            # For unitless measurements, use the result value directly (no conversion needed)
            conversion_case_sql = "obs.RESULT_VALUE"
        else:
            conversion_case_sql = f"""
                CASE
                    {' '.join(conversion_cases)}
                    ELSE NULL
                END
            """


        upper_limit = config.upper_limit if config.upper_limit is not None else 1e10
        lower_limit = config.lower_limit if config.lower_limit is not None else 0

        query = f"""
        SELECT
            obs.PERSON_ID,
            obs.CLINICAL_EFFECTIVE_DATE,
            obs.AGE_AT_EVENT,
            def.DEFINITION_ID,
            def.DEFINITION_NAME,
            def.DEFINITION_VERSION,
            def.VERSION_DATETIME,
            obs.RESULT_VALUE AS SOURCE_RESULT_VALUE,
            COALESCE(obs.RESULT_VALUE_UNIT, 'No Unit') AS SOURCE_RESULT_VALUE_UNITS,
            obs.OBSERVATION_CONCEPT_CODE AS SOURCE_CONCEPT_CODE,
            obs.OBSERVATION_CONCEPT_NAME AS SOURCE_CONCEPT_NAME,
            obs.OBSERVATION_CONCEPT_VOCABULARY AS SOURCE_CONCEPT_VOCABULARY,
            {conversion_case_sql.replace('mapped_unit', f'({mapping_case_sql})')} AS VALUE_AS_NUMBER,
            '{config.primary_standard_unit}' AS VALUE_UNITS,
            CASE WHEN {conversion_case_sql.replace('mapped_unit', f'({mapping_case_sql})')} > {upper_limit}
                THEN 1 ELSE 0 END AS ABOVE_RANGE,
            CASE WHEN {conversion_case_sql.replace('mapped_unit', f'({mapping_case_sql})')} < {lower_limit}
                THEN 1 ELSE 0 END AS BELOW_RANGE
        FROM {st.session_state.config["int_observation_table"]} obs
        INNER JOIN {st.session_state.config["definition_library"]["database"]}.
            {st.session_state.config["definition_library"]["schema"]}.DEFINITIONSTORE def
            ON obs.OBSERVATION_CONCEPT_CODE = def.CODE
            AND obs.OBSERVATION_CONCEPT_VOCABULARY = def.VOCABULARY
        WHERE def.DEFINITION_NAME = '{definition_name}'
            AND obs.RESULT_VALUE IS NOT NULL
            AND TRY_CAST(obs.RESULT_VALUE AS FLOAT) IS NOT NULL
            AND ({mapping_case_sql}) IS NOT NULL
            AND ({conversion_case_sql.replace('mapped_unit', f'({mapping_case_sql})')}) IS NOT NULL
            AND def.VERSION_DATETIME = (
                SELECT MAX(VERSION_DATETIME)
                FROM {st.session_state.config["definition_library"]["database"]}.
                    {st.session_state.config["definition_library"]["schema"]}.DEFINITIONSTORE
                WHERE DEFINITION_NAME = '{definition_name}'
            )
            AND YEAR(obs.CLINICAL_EFFECTIVE_DATE) BETWEEN 2000 AND YEAR(CURRENT_DATE())
        """

        union_queries.append(query)

    if not union_queries:
        return None

    final_query = " UNION ALL ".join(union_queries)

    return final_query


def _initialize_base_measurements_table():
    """
    Initialize the base measurements table structure
    """
    st.session_state.session.sql(f"""
        CREATE OR REPLACE TABLE {st.session_state.config["feature_store"]["database"]}.
        {st.session_state.config["feature_store"]["schema"]}.DEV_MEASUREMENTS (
            PERSON_ID VARCHAR,
            CLINICAL_EFFECTIVE_DATE TIMESTAMP_NTZ,
            AGE_AT_EVENT INTEGER,
            DEFINITION_ID VARCHAR,
            DEFINITION_NAME VARCHAR,
            DEFINITION_VERSION VARCHAR,
            VERSION_DATETIME TIMESTAMP_NTZ,
            SOURCE_RESULT_VALUE FLOAT,
            SOURCE_RESULT_VALUE_UNITS VARCHAR,
            SOURCE_CONCEPT_CODE VARCHAR,
            SOURCE_CONCEPT_NAME VARCHAR,
            SOURCE_CONCEPT_VOCABULARY VARCHAR,
            VALUE_AS_NUMBER FLOAT,
            VALUE_UNITS VARCHAR,
            ABOVE_RANGE BOOLEAN,
            BELOW_RANGE BOOLEAN
        )
    """).collect()


def create_base_measurements_feature_incremental(eligible_configs):
    """
    Create or update Base Measurements feature table incrementally
    This prevents timeouts from massive single query
    """
    try:
        with st.spinner("Initializing Base Measurements table structure..."):
            _initialize_base_measurements_table()

        # process each measurement definition individually
        progress_bar = st.progress(0, f"Processing 0 of {len(eligible_configs)} measurements")
        status_text = st.empty()

        successful_measurements = []
        failed_measurements = []

        for i, (definition_name, config) in enumerate(eligible_configs.items()):
            try:
                status_text.info(f"Processing measurement: **{definition_name}**")

                single_config = {definition_name: config}
                sql_query = create_base_measurements_sql(single_config)

                if sql_query:
                    st.session_state.session.sql(
                        f"""INSERT INTO {st.session_state.config["feature_store"]["database"]}.
                        {st.session_state.config["feature_store"]["schema"]}.DEV_MEASUREMENTS
                        {sql_query}""").collect()

                    successful_measurements.append(definition_name)
                else:
                    failed_measurements.append((definition_name, "No SQL generated"))

            except Exception as e:
                failed_measurements.append((definition_name, e))
                st.warning(f"Failed to process {definition_name}: {e}")

            # update progress bar
            progress = (i + 1) / len(eligible_configs)
            progress_bar.progress(progress, f"Processed {i + 1} of {len(eligible_configs)} measurements")

        # clear statis
        progress_bar.empty()
        status_text.empty()

        if successful_measurements:
            st.success(f"Base Measurements feature table updated! "
                      f"Successfully processed {len(successful_measurements)} measurements.")

        if failed_measurements:
            st.warning(f"{len(failed_measurements)} measurements failed to process:")
            for def_name, error in failed_measurements:
                st.write(f"• {def_name}: {error}")

    except Exception as e:
        st.error(f"Error creating Base Measurements feature table: {e}")


# def create_base_measurements_feature(eligible_configs):
#     """
#     Create or update Base Measurements feature table using FeatureStoreManager
#     DEPRECATED: Use create_base_measurements_feature_incremental instead to avoid timeouts
#     """
#     try:
#         with st.spinner("Generating SQL query..."):
#             sql_query = create_base_measurements_sql(eligible_configs)

#         if not sql_query:
#             st.error("Failed to generate SQL query. No eligible measurements found.")
#             return

#         with st.spinner("Creating or updating Base Measurements feature table..."):
#             # st.session_state.session.sql(
#             # f"""CREATE TABLE IF NOT EXISTS {st.session_state.config["feature_store"]["database"]}.
#             # {st.session_state.config["feature_store"]["schema"]}.BASE_MEASUREMENTS(
#             # CLINICAL_EFFECTIVE_DATE TIMESTAMP_NTZ,
#             # DEFINITION_ID VARCHAR,
#             # DEFINITION_NAME VARCHAR,
#             # DEFINITION_VERSION VARCHAR,
#             # VERSION_DATETIME TIMESTAMP_NTZ,
#             # PERSON_ID VARCHAR,
#             # SOURCE_RESULT_VALUE FLOAT,
#             # SOURCE_RESULT_VALUE_UNITS VARCHAR,
#             # VALUE_AS_NUMBER FLOAT,
#             # VALUE_UNITS VARCHAR
#             # )""").collect()

#             # st.session_state.session.sql(
#             # f"""MERGE INTO {st.session_state.config["feature_store"]["database"]}.
#             # {st.session_state.config["feature_store"]["schema"]}.BASE_MEASUREMENTS AS target
#             # USING ({sql_query}) AS source
#             # ON target.PERSON_ID = source.PERSON_ID
#             # AND target.CLINICAL_EFFECTIVE_DATE = source.CLINICAL_EFFECTIVE_DATE
#             # AND target.DEFINITION_ID = source.DEFINITION_ID
#             # AND target.SOURCE_RESULT_VALUE = source.SOURCE_RESULT_VALUE
#             # AND target.SOURCE_RESULT_VALUE_UNITS = source.SOURCE_RESULT_VALUE_UNITS
#             # WHEN NOT MATCHED THEN
#             #     INSERT (CLINICAL_EFFECTIVE_DATE, DEFINITION_ID, DEFINITION_NAME, PERSON_ID,
#             #             SOURCE_RESULT_VALUE, SOURCE_RESULT_VALUE_UNITS, VALUE_AS_NUMBER, VALUE_UNITS)
#             #     VALUES (source.CLINICAL_EFFECTIVE_DATE, source.DEFINITION_ID, source.DEFINITION_NAME, source.PERSON_ID,
#             #             source.SOURCE_RESULT_VALUE, source.SOURCE_RESULT_VALUE_UNITS, source.VALUE_AS_NUMBER,
#             #             source.VALUE_UNITS)""").collect()

#             st.session_state.session.sql(
#                 f"""CREATE OR REPLACE TABLE {st.session_state.config["feature_store"]["database"]}.
#                 {st.session_state.config["feature_store"]["schema"]}.DEV_MEASUREMENTS AS
#                 {sql_query}""").collect()

#             st.success("Base Measurements feature table created or updated successfully!")

        # with st.spinner("Initialising Feature Store Manager..."):
        #     feature_manager = FeatureStoreManager(
        #         connection=st.session_state.session,
        #         database=st.session_state.config["feature_store"]["database"],
        #         schema=st.session_state.config["feature_store"]["schema"],
        #         metadata_schema=st.session_state.config["feature_store"]["metadata_schema"]
        #     )

        # feature_name = "BASE_MEASUREMENTS"
        # feature_desc = f"Standardised measurement data from {len(eligible_configs)} measurement definitions with unit conversions applied"
        # feature_format = "tabular"

        # with st.spinner("Creating or updating Base Measurements feature table..."):
        #     st.session_state.session.use_database(st.session_state.config["feature_store"]["database"])
        #     st.session_state.session.use_schema(st.session_state.config["feature_store"]["metadata_schema"])
        #     feature_id_result = st.session_state.session.sql(f"""
        #             SELECT feature_id FROM feature_registry
        #             WHERE feature_name = '{feature_name}'
        #         """).collect()

        #     if feature_id_result:
        #         st.info("Feature already exists. Updating with new data...")
        #         existing_feature_id = feature_id_result[0]["FEATURE_ID"]

        #         feature_version, table_name = feature_manager.update_feature(
        #             feature_id=existing_feature_id,
        #             new_sql_select_query=sql_query,
        #             change_description=f"Updated with {len(eligible_configs)} measurement definitions",
        #             force_new_version=True
        #         )

        #         st.success(f"Base Measurements feature updated successfully!")
        #         st.write(f"**Feature ID:** {existing_feature_id}")
        #         st.write(f"**New Version:** {feature_version}")
        #         st.write(f"**Table Name:** {table_name}")
        #     else:
        #         feature_id, feature_version = feature_manager.add_new_feature(
        #             feature_name=feature_name,
        #             feature_desc=feature_desc,
        #             feature_format=feature_format,
        #             sql_select_query_to_generate_feature=sql_query
        #         )

        #         st.success(f"Base Measurements feature created successfully!")
        #         st.write(f"**Feature ID:** {feature_id}")
        #         st.write(f"**Feature Version:** {feature_version}")

        #         table_name = f"{feature_name}_V{feature_version}"
        #         st.write(f"**Table Name:** {table_name}")

        #     try:
        #         count_result = st.session_state.session.sql(
        #             f"""SELECT COUNT(*) as row_count FROM {st.session_state.config["feature_store"]["database"]}.
        #                 {st.session_state.config["feature_store"]["schema"]}.DEV_MEASUREMENTS""").to_pandas()
        #         row_count = count_result.iloc[0]['ROW_COUNT']
        #         st.write(f"**Rows Created:** {row_count:,}")
        #     except Exception as e:
        #         st.warning(f"Could not get row count: {e}")

    # except Exception as e:
    #     st.error(f"Error creating Base Measurements feature: {e}")
    #     st.exception(e)

def create_measurement_configs_tables(config: Optional[dict] = None, session: Optional[Session] = None):
    """
    Create tables for measurement configurations in Snowflake, if they don't already exist.

    Args:
        config (Optional[dict]): Configuration dictionary containing database and schema information. If not provided,
            will use the session state from Streamlit. Leave as None if calling from within Streamlit app.
        session (Optional[Session]): Snowflake session object. If not provided, will use the session state from Streamlit.
            Leave as None if calling from within Streamlit app.
    """
    config = config or st.session_state.config
    session = session or st.session_state.session

    queries = [
    f"""
    CREATE TABLE IF NOT EXISTS {config["measurement_configs"]["database"]}.
        {config["measurement_configs"]["schema"]}.MEASUREMENT_CONFIGS (
            DEFINITION_ID VARCHAR,
            DEFINITION_NAME VARCHAR,
            CONFIG_ID VARCHAR,
            CONFIG_VERSION VARCHAR
        )""",
    f"""
    CREATE TABLE IF NOT EXISTS {config["measurement_configs"]["database"]}.
        {config["measurement_configs"]["schema"]}.STANDARD_UNITS (
            DEFINITION_ID VARCHAR,
            DEFINITION_NAME VARCHAR,
            CONFIG_ID VARCHAR,
            CONFIG_VERSION VARCHAR,
            UNIT VARCHAR,
            PRIMARY_UNIT BOOLEAN
        )""",
    f"""
    CREATE TABLE IF NOT EXISTS {config["measurement_configs"]["database"]}.
        {config["measurement_configs"]["schema"]}.UNIT_MAPPINGS (
            DEFINITION_ID VARCHAR,
            DEFINITION_NAME VARCHAR,
            CONFIG_ID VARCHAR,
            CONFIG_VERSION VARCHAR,
            SOURCE_UNIT VARCHAR,
            STANDARD_UNIT VARCHAR,
            SOURCE_UNIT_COUNT INTEGER,
            SOURCE_UNIT_LQ FLOAT,
            SOURCE_UNIT_MEDIAN FLOAT,
            SOURCE_UNIT_UQ FLOAT
        )""",
    f"""
    CREATE TABLE IF NOT EXISTS {config["measurement_configs"]["database"]}.
        {config["measurement_configs"]["schema"]}.UNIT_CONVERSIONS (
            DEFINITION_ID VARCHAR,
            DEFINITION_NAME VARCHAR,
            CONFIG_ID VARCHAR,
            CONFIG_VERSION VARCHAR,
            CONVERT_FROM_UNIT VARCHAR,
            CONVERT_TO_UNIT VARCHAR,
            PRE_OFFSET FLOAT,
            MULTIPLY_BY FLOAT,
            POST_OFFSET FLOAT
        )""",
    f"""
    CREATE TABLE IF NOT EXISTS {config["measurement_configs"]["database"]}.
        {config["measurement_configs"]["schema"]}.VALUE_BOUNDS (
            DEFINITION_ID VARCHAR,
            DEFINITION_NAME VARCHAR,
            CONFIG_ID VARCHAR,
            CONFIG_VERSION VARCHAR,
            LOWER_LIMIT FLOAT,
            UPPER_LIMIT FLOAT
    )"""]
    for query in queries:
        session.sql(query).collect()
    print("Measurement config tables created")

def load_measurement_configs_into_tables(config: Optional[dict] = None, session: Optional[Session] = None):

    """
    Takes all the existing measurement config files in /data/measurements/{icb_name}/,
    deletes any existing entries in the tables for that definition, and then inserts the new entries.

    Args:
        config (Optional[dict]): Configuration dictionary containing database and schema information. If not provided,
            will use the session state from Streamlit. Leave as None if calling from within Streamlit app.
        session (Optional[Session]): Snowflake session object. If not provided, will use the session state from Streamlit.
            Leave as None if calling from within Streamlit app.
    """
    config = config or st.session_state.config
    session = session or st.session_state.session

    config_files = load_measurement_configs_list(config=config)
    total_configs = len(config_files)


    for config_file in config_files:
        measurement_config = load_measurement_config(filename=config_file, config=config)
        # print(config.definition_name)
        standard_units, unit_mappings, unit_conversions, value_bounds = measurement_config.to_dataframes()

        # Delete all existing entries for this definition
        queries = [f"""DELETE FROM {config["measurement_configs"]["database"]}.
            {config["measurement_configs"]["schema"]}.MEASUREMENT_CONFIGS
            WHERE DEFINITION_NAME = '{measurement_config.definition_name}'""",
                f"""DELETE FROM {config["measurement_configs"]["database"]}.
            {config["measurement_configs"]["schema"]}.STANDARD_UNITS
            WHERE DEFINITION_NAME = '{measurement_config.definition_name}'""",
                   f"""DELETE FROM {config["measurement_configs"]["database"]}.
            {config["measurement_configs"]["schema"]}.UNIT_MAPPINGS
            WHERE DEFINITION_NAME = '{measurement_config.definition_name}'""",
                   f"""DELETE FROM {config["measurement_configs"]["database"]}.
            {config["measurement_configs"]["schema"]}.UNIT_CONVERSIONS
            WHERE DEFINITION_NAME = '{measurement_config.definition_name}'""",
                f"""DELETE FROM {config["measurement_configs"]["database"]}.
            {config["measurement_configs"]["schema"]}.VALUE_BOUNDS
            WHERE DEFINITION_NAME = '{measurement_config.definition_name}'"""]

        for query in queries:

            session.sql(query).collect()

        session.sql(f"""INSERT INTO {config["measurement_configs"]["database"]}.
            {config["measurement_configs"]["schema"]}.MEASUREMENT_CONFIGS
            (DEFINITION_ID, DEFINITION_NAME, CONFIG_ID, CONFIG_VERSION)
            VALUES (
                '{measurement_config.definition_id}',
                '{measurement_config.definition_name}',
                '{measurement_config.standard_measurement_config_id}',
                '{measurement_config.standard_measurement_config_version}'
            )""").collect()

        if not standard_units.empty:
            session.write_pandas(standard_units,
                database=config["measurement_configs"]["database"],
                schema=config["measurement_configs"]["schema"],
                table_name="STANDARD_UNITS",
                use_logical_type=True)
        if not unit_mappings.empty:
            session.write_pandas(unit_mappings,
                database=config["measurement_configs"]["database"],
                schema=config["measurement_configs"]["schema"],
                table_name="UNIT_MAPPINGS",
                use_logical_type=True)
        if not unit_conversions.empty:
            session.write_pandas(unit_conversions,
                database=config["measurement_configs"]["database"],
                schema=config["measurement_configs"]["schema"],
                table_name="UNIT_CONVERSIONS",
                use_logical_type=True)
        if not value_bounds.empty:
            session.write_pandas(value_bounds,
                database=config["measurement_configs"]["database"],
                schema=config["measurement_configs"]["schema"],
                table_name="VALUE_BOUNDS",
                use_logical_type=True)

        print(f"Loaded {config_file} for {measurement_config.definition_id} into measurement config tables")

    return total_configs

def count_sigfig(number: float,
                zeros: int = 4,
                nines: int = 5,
                max_sigfig: int = 8,
                ) -> int:
    """Function to count number of significant figures
    number: any real number
    zeros: number of zeros in a row that after which it will stop counting
    nines: number of consecu
    """
    num_tuple = Decimal(number).normalize().as_tuple().digits
    nzeros = nnines = sigfig = 0
    if not number == 0:
        while nzeros < zeros and nnines < nines and sigfig < max_sigfig and sigfig < len(num_tuple):
            if num_tuple[sigfig] == 0:
                nzeros += 1
                nnines = 0
            elif num_tuple[sigfig] == 9:
                nnines += 1
                nzeros = 0
            else:
                nnines = nzeros = 0
            sigfig += 1
        return sigfig - max(nzeros, nnines)
    else:
        return 1

