import json
import os
from typing import List

import streamlit as st


def get_non_measurement_definitions():
    """
    Get all non-measurement definitions from the definitions folder
    """
    definitions = {}
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
    return definitions


def get_latest_base_apc_concepts_table():
    """
    Get the latest version of BASE_APC_CONCEPTS table
    """
    query = f"""
    SELECT TABLE_NAME
    FROM {st.session_state.config["feature_store"]["database"]}.INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = '{st.session_state.config["feature_store"]["schema"]}'
      AND TABLE_NAME LIKE 'BASE_APC_CONCEPTS%'
    ORDER BY TABLE_NAME DESC
    LIMIT 1
    """
    result = st.session_state.session.sql(query).to_pandas()
    if result.empty:
        return None
    return result.iloc[0]['TABLE_NAME']


def create_base_conditions_sql(selected_definitions: List[str]):
    """
    Generate SQL query for Base Conditions feature table
    Handles both SNOMED codes (from OBSERVATION) and ICD10/OPCS4 codes (from BASE_APC_CONCEPTS)
    """
    # Get latest BASE_APC_CONCEPTS table
    apc_table = get_latest_base_apc_concepts_table()
    if not apc_table:
        st.warning("BASE_APC_CONCEPTS table not found. ICD10/OPCS4 codes will not be included.")

    union_queries = []

    for definition_name in selected_definitions:
        # SNOMED codes from OBSERVATION table
        snomed_query = f"""
        SELECT DISTINCT
            obs.PERSON_ID,
            obs.CLINICAL_EFFECTIVE_DATE AS CLINICAL_EFFECTIVE_DATE,
            def.DEFINITION_ID,
            def.DEFINITION_NAME,
            'SNOMED' AS SOURCE_VOCABULARY
        FROM {st.session_state.config["gp_observation_table"]} obs
        LEFT JOIN {st.session_state.config["definition_library"]["database"]}.
            {st.session_state.config["definition_library"]["schema"]}.DEFINITIONSTORE def
            ON obs.CORE_CONCEPT_ID = def.DBID
        WHERE def.DEFINITION_NAME = '{definition_name}'
            AND def.VOCABULARY = 'SNOMED'
        """
        union_queries.append(snomed_query)

        # ICD10/OPCS4 codes from BASE_APC_CONCEPTS table
        if apc_table:
            icd_opcs_query = f"""
            SELECT DISTINCT
                apc.PERSON_ID,
                apc.ACTIVITY_DATE AS CLINICAL_EFFECTIVE_DATE,
                def.DEFINITION_ID,
                def.DEFINITION_NAME,
                apc.VOCABULARY AS SOURCE_VOCABULARY
            FROM {st.session_state.config["feature_store"]["database"]}.
                {st.session_state.config["feature_store"]["schema"]}.{apc_table} apc
            INNER JOIN {st.session_state.config["definition_library"]["database"]}.
                {st.session_state.config["definition_library"]["schema"]}.DEFINITIONSTORE def
                ON apc.VOCABULARY = def.VOCABULARY
                AND apc.CONCEPT_CODE_STD = def.CODE
            WHERE def.DEFINITION_NAME = '{definition_name}'
                AND def.VOCABULARY IN ('ICD10', 'OPCS4')
            """
            union_queries.append(icd_opcs_query)

    if not union_queries:
        return None

    return " UNION ALL ".join(union_queries)


def create_base_conditions_feature(selected_definitions: List[str]):   
    """
    Create or update Base Conditions ('Has Condition') feature table
    """
    try:
        with st.spinner("Generating SQL query..."):
            sql_query = create_base_conditions_sql(selected_definitions)

        if not sql_query:
            st.error("Failed to generate SQL query. No definitions selected.")
            return

        with st.spinner("Creating or updating Base Conditions feature table..."):
            st.session_state.session.sql(
                f"""CREATE TABLE IF NOT EXISTS {st.session_state.config["feature_store"]["database"]}.
                {st.session_state.config["feature_store"]["schema"]}.BASE_CONDITIONS(
                PERSON_ID VARCHAR,
                CLINICAL_EFFECTIVE_DATE TIMESTAMP_NTZ,
                DEFINITION_ID VARCHAR,
                DEFINITION_NAME VARCHAR,
                SOURCE_VOCABULARY VARCHAR
                )""").collect()
            
            st.session_state.session.sql(
                f"""MERGE INTO {st.session_state.config["feature_store"]["database"]}.
                {st.session_state.config["feature_store"]["schema"]}.BASE_CONDITIONS AS target
                USING ({sql_query}) AS source
                ON target.PERSON_ID = source.PERSON_ID
                AND target.CLINICAL_EFFECTIVE_DATE = source.CLINICAL_EFFECTIVE_DATE
                AND target.DEFINITION_ID = source.DEFINITION_ID
                AND source.SOURCE_VOCABULARY = target.SOURCE_VOCABULARY
                WHEN NOT MATCHED THEN
                    INSERT (PERSON_ID, CLINICAL_EFFECTIVE_DATE, DEFINITION_ID, DEFINITION_NAME, SOURCE_VOCABULARY)
                    VALUES (source.PERSON_ID, source.CLINICAL_EFFECTIVE_DATE, source.DEFINITION_ID, source.DEFINITION_NAME, source.SOURCE_VOCABULARY)""").collect()
            st.success(f"Base Conditions feature created or updated successfully!")


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

            try:
                count_result = st.session_state.session.sql(
                    f"""SELECT COUNT(*) as row_count FROM {st.session_state.config["feature_store"]["database"]}.
                        {st.session_state.config["feature_store"]["schema"]}.BASE_CONDITIONS""").to_pandas()
                row_count = count_result.iloc[0]['ROW_COUNT']
                st.write(f"**Rows Created:** {row_count:,}")
            except Exception as e:
                st.warning(f"Could not get row count: {e}")

    except Exception as e:
        st.error(f"Error creating Base Conditions feature: {e}")
        st.exception(e)
