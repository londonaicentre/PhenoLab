import json
import os
from typing import List

import streamlit as st


def get_non_measurement_definitions(source="AIC"):
    """
    Get all non-measurement definitions from AI Centre (AIC) or Snowflake (ICB)

    Args:
        source (str):
            "AIC" for AI Centre definitions
            "ICB" for ICB definitions

    Returns:
        dict:
            Dictionary of definition_name -> definition data
    """
    definitions = {}

    if source == "AIC":
        # Load from local files (existing behavior)
        if os.path.exists("data/definitions"):
            for filename in os.listdir("data/definitions"):
                if filename.endswith(".json") and not filename.startswith("measurement_"):
                    filepath = os.path.join("data/definitions", filename)
                    try:
                        with open(filepath, 'r') as f:
                            definition = json.load(f)
                            definitions[definition['definition_name']] = definition
                    except Exception as e:
                        st.warning(f"Could not load {filename}: {e}")

    elif source == "ICB":
        # Load from Snowflake DEFINITIONSTORE
        try:
            query = f"""
            SELECT DISTINCT DEFINITION_NAME
            FROM {st.session_state.config["definition_library"]["database"]}.
                {st.session_state.config["definition_library"]["schema"]}.DEFINITIONSTORE
            WHERE SOURCE_TABLE = 'ICB_DEFINITIONS'
                AND DEFINITION_NAME NOT LIKE 'measurement_%'
            ORDER BY DEFINITION_NAME
            """
            result = st.session_state.session.sql(query).to_pandas()

            # Create simplified definition dict with just the name (sufficient for base feature creation)
            for _, row in result.iterrows():
                definitions[row['DEFINITION_NAME']] = {
                    'definition_name': row['DEFINITION_NAME'],
                    'source': 'ICB'
                }
        except Exception as e:
            st.warning(f"Could not load ICB definitions from Snowflake: {e}")

    return definitions


def create_base_conditions_sql(selected_definitions: List[str], source="AIC"):
    """
    Generate SQL query for Base Conditions feature table using the unified INT_OBSERVATION table.
    This single query replaces the previous three-way UNION approach.

    Args:
        selected_definitions: List of definition names to include
        source: "AIC" for AI Centre definitions, "ICB" for ICB definitions
    """
    if not selected_definitions:
        return None

    definition_list = "', '".join(selected_definitions)

    query = f"""
    SELECT DISTINCT
        obs.PERSON_ID,
        obs.CLINICAL_EFFECTIVE_DATE,
        def.DEFINITION_ID,
        def.DEFINITION_NAME,
        def.DEFINITION_VERSION,
        def.VERSION_DATETIME,
        obs.OBSERVATION_CONCEPT_CODE AS SOURCE_CONCEPT_CODE,
        obs.OBSERVATION_CONCEPT_NAME AS SOURCE_CONCEPT_NAME,
        obs.OBSERVATION_CONCEPT_VOCABULARY AS SOURCE_CONCEPT_VOCABULARY
    FROM {st.session_state.config["int_observation_table"]} obs
    INNER JOIN {st.session_state.config["definition_library"]["database"]}.
        {st.session_state.config["definition_library"]["schema"]}.DEFINITIONSTORE def
        ON obs.OBSERVATION_CONCEPT_CODE = def.CODE
        AND obs.OBSERVATION_CONCEPT_VOCABULARY = def.VOCABULARY
    WHERE def.DEFINITION_NAME IN ('{definition_list}')
        AND def.VERSION_DATETIME = (
            SELECT MAX(d2.VERSION_DATETIME)
            FROM {st.session_state.config["definition_library"]["database"]}.
                {st.session_state.config["definition_library"]["schema"]}.DEFINITIONSTORE d2
            WHERE d2.DEFINITION_NAME = def.DEFINITION_NAME
        )
        AND def.SOURCE_TABLE = '{"AIC_DEFINITIONS" if source == "AIC" else "ICB_DEFINITIONS"}'
        AND YEAR(obs.CLINICAL_EFFECTIVE_DATE) BETWEEN 2000 AND YEAR(CURRENT_DATE())
    """

    return query


def _initialize_base_conditions_table(table_name: str):
    """
    Initialize the base conditions table structure

    Args:
        table_name: Name of the table to create
    """
    st.session_state.session.sql(f"""
        CREATE OR REPLACE TABLE {st.session_state.config["feature_store"]["database"]}.
        {st.session_state.config["feature_store"]["schema"]}.{table_name} (
            PERSON_ID VARCHAR,
            CLINICAL_EFFECTIVE_DATE TIMESTAMP_NTZ,
            DEFINITION_ID VARCHAR,
            DEFINITION_NAME VARCHAR,
            DEFINITION_VERSION VARCHAR,
            VERSION_DATETIME TIMESTAMP_NTZ,
            SOURCE_CONCEPT_CODE VARCHAR,
            SOURCE_CONCEPT_NAME VARCHAR,
            SOURCE_CONCEPT_VOCABULARY VARCHAR
        )
    """).collect()


def create_base_conditions_feature_incremental(selected_definitions: List[str], source="AIC"):
    """
    Create or update Base Conditions ('Has Condition') feature table incrementally
    This prevents timeouts from massive single query
    """
    try:
        # AIC or ICB
        table_name = "DEV_ICB_CONDITIONS" if source == "ICB" else "DEV_AIC_CONDITIONS"
        table_display_name = "Dev ICB Conditions" if source == "ICB" else "Dev AIC Conditions"

        with st.spinner(f"Initializing {table_display_name} table structure..."):
            _initialize_base_conditions_table(table_name)

        # process each individually
        progress_bar = st.progress(0, f"Processing 0 of {len(selected_definitions)} definitions")
        status_text = st.empty()

        successful_definitions = []
        failed_definitions = []

        for i, definition_name in enumerate(selected_definitions):
            try:
                status_text.info(f"Processing definition: **{definition_name}**")

                sql_query = create_base_conditions_sql([definition_name], source=source)

                if sql_query:
                    st.session_state.session.sql(
                        f"""INSERT INTO {st.session_state.config["feature_store"]["database"]}.
                        {st.session_state.config["feature_store"]["schema"]}.{table_name}
                        {sql_query}""").collect()

                    successful_definitions.append(definition_name)
                else:
                    failed_definitions.append((definition_name, "No SQL generated"))

            except Exception as e:
                failed_definitions.append((definition_name, e))
                st.warning(f"Failed to process {definition_name}: {e}")

            # update progress
            progress = (i + 1) / len(selected_definitions)
            progress_bar.progress(progress, f"Processed {i + 1} of {len(selected_definitions)} definitions")

        progress_bar.empty()
        status_text.empty()

        if successful_definitions:
            st.success(f"{table_display_name} feature table updated! "
                      f"Successfully processed {len(successful_definitions)} definitions.")

        if failed_definitions:
            st.warning(f"{len(failed_definitions)} definitions failed to process:")
            for def_name, error in failed_definitions:
                st.write(f"• {def_name}: {error}")

    except Exception as e:
        st.error(f"Error creating {table_display_name} feature table: {e}")


# def create_base_conditions_feature(selected_definitions: List[str], source="AIC"):
#     """
#     Create or update Base Conditions ('Has Condition') feature table
#     DEPRECATED: Use create_base_conditions_feature_incremental instead to avoid timeouts

#     Args:
#         selected_definitions:
#             List of definition names to include
#         source:
#             "AIC" for AI Centre definitions (creates BASE_CONDITIONS),
#             "ICB" for ICB definitions (creates BASE_ICB_CONDITIONS)
#     """
#     try:
#         with st.spinner("Generating SQL query..."):
#             sql_query = create_base_conditions_sql(selected_definitions, source=source)

#         if not sql_query:
#             st.error("Failed to generate SQL query. No definitions selected.")
#             return

#         # Determine table name based on source
#         table_name = "DEV_ICB_CONDITIONS" if source == "ICB" else "DEV_AIC_CONDITIONS"
#         table_display_name = "Dev ICB Conditions" if source == "ICB" else "Dev AIC Conditions"

#         with st.spinner(f"Creating or updating {table_display_name} feature table..."):
#             st.session_state.session.sql(
#                 f"""CREATE OR REPLACE TABLE {st.session_state.config["feature_store"]["database"]}.
#                 {st.session_state.config["feature_store"]["schema"]}.{table_name} AS
#                 {sql_query}""").collect()

#             st.success(f"{table_display_name} feature created or updated successfully!")


        # with st.spinner("Initialising Feature Store Manager..."):
        #     feature_manager = FeatureStoreManager(
        #         connection=st.session_state.session,
        #         database=st.session_state.config["feature_store"]["database"],
        #         schema=st.session_state.config["feature_store"]["schema"],
        #         metadata_schema=st.session_state.config["feature_store"]["metadata_schema"],
        #     )

        # feature_name = "BASE_CONDITIONS"
        # feature_desc = f"Condition flags from {len(selected_definitions)} non-measurement definitions"
        # feature_format = "tabular"

        # with st.spinner("Creating or updating Base Conditions feature table..."):
        #     feature_id_result = st.session_state.session.sql(f"""
        #         SELECT feature_id FROM {st.session_state.config["feature_store"]["database"]}.
        #             {st.session_state.config["feature_store"]["metadata_schema"]}.feature_registry
        #         WHERE feature_name = '{feature_name}'""").collect()

        #     if feature_id_result:
        #         st.info("Feature already exists. Updating with new data...")
        #         existing_feature_id = feature_id_result[0]["FEATURE_ID"]

        #         feature_version, table_name = feature_manager.update_feature(
        #             feature_id=existing_feature_id,
        #             new_sql_select_query=sql_query,
        #             change_description=f"Updated with {len(selected_definitions)} condition definitions",
        #             force_new_version=True
        #         )

        #         st.success(f"Base Conditions feature updated successfully!")
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

        #         st.success(f"Base Conditions feature created successfully!")
        #         st.write(f"**Feature ID:** {feature_id}")
        #         st.write(f"**Feature Version:** {feature_version}")

        #         table_name = f"{feature_name}_V{feature_version}"
        #         st.write(f"**Table Name:** {table_name}")

        #     try:
        #         count_result = st.session_state.session.sql(
        #             f"""SELECT COUNT(*) as row_count FROM {st.session_state.config["feature_store"]["database"]}.
        #                 {st.session_state.config["feature_store"]["schema"]}.{table_name}""").to_pandas()
        #         row_count = count_result.iloc[0]['ROW_COUNT']
        #         st.write(f"**Rows Created:** {row_count:,}")
        #     except Exception as e:
        #         st.warning(f"Could not get row count: {e}")

    # except Exception as e:
    #     st.error(f"Error creating Base Conditions feature: {e}")
    #     st.exception(e)
