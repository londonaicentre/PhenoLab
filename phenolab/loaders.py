"""
Contains all functions for loading definitions and measurement configurations.
"""
import os
from datetime import datetime
from typing import List, Tuple

import pandas as pd
from create_tables import load_definitions_to_snowflake
from utils.definition import Definition
from utils.definition_interaction_utils import load_definitions_list_from_local_files
from utils.measurement import load_measurement_config_from_json

### AI CENTRE DEFINITIONS

def process_definitions_for_upload(definition_files: List[str], session, config) -> Tuple[pd.DataFrame, List[str], dict]:
    """
    Process all definition files and prepare them for upload to Snowflake
    """
    if not definition_files:
        return pd.DataFrame(), [], {}

    all_rows = pd.DataFrame()
    definitions_to_remove = {}
    definitions_to_add = []

    for def_file in definition_files:
        file_path = os.path.join("data/definitions", def_file)
        definition = Definition.from_json(file_path)

        query = f"""
        SELECT DEFINITION_ID, DEFINITION_NAME, VERSION_DATETIME
        FROM {config["definition_library"]["database"]}.
        {config["definition_library"]["schema"]}.
        AIC_DEFINITIONS
        WHERE DEFINITION_ID = '{definition.definition_id}'
        """
        existing_definition = session.sql(query).to_pandas()

        if not existing_definition.empty:
            max_version_in_db = existing_definition["VERSION_DATETIME"].max()
            current_version = definition.version_datetime

            if current_version == max_version_in_db:
                continue  # skip if already exists

            if current_version < max_version_in_db:
                continue  # skip if newer version exists

            # record that we want to delete the old one
            definitions_to_remove[definition.definition_id] = [definition.definition_name, current_version]

        definition.uploaded_datetime = datetime.now()

        all_rows = pd.concat([all_rows, definition.to_dataframe()])
        definitions_to_add.append(definition.definition_name)

    return all_rows, definitions_to_add, definitions_to_remove


def update_aic_definitions_local(session, config):
    """
    Update the AIC_DEFINITIONS table with new or updated definitions from local files (local version)
    """
    definition_files = load_definitions_list_from_local_files()

    if not definition_files:
        print("No AIC definition files found")
        return

    print(f"Processing {len(definition_files)} definition files...")
    all_rows, definitions_to_add, definitions_to_remove = process_definitions_for_upload(definition_files, session, config)

    # Upload if there's data
    if not all_rows.empty:
        print(f"Uploading {len(all_rows)} rows to Snowflake...")
        df = all_rows.copy()
        df.columns = df.columns.str.upper()
        session.write_pandas(df,
                            database=config["definition_library"]["database"],
                            schema=config["definition_library"]["schema"],
                            table_name="AIC_DEFINITIONS",
                            overwrite=False,
                            use_logical_type=True)
        print(f"Uploaded {len(all_rows)} rows to AIC_DEFINITIONS table")

        # Delete old versions
        for definition_id, [name, current_version] in definitions_to_remove.items():
            session.sql(
                f"""DELETE FROM {config["definition_library"]["database"]}.
                {config["definition_library"]["schema"]}.
                AIC_DEFINITIONS WHERE DEFINITION_ID = '{definition_id}' AND
                VERSION_DATETIME != CAST('{current_version}' AS TIMESTAMP)"""
            ).collect()
            print(f"Deleted old version of definition {name}")

        print(f"Successfully uploaded definitions: {definitions_to_add}")
    else:
        print("No new AIC definitions to upload")

### EXTERNAL DEFINITIONS

EXTERNAL_DEFINITION_SOURCES = {
    "HDRUK_DEFINITIONS": "_external_definitions/data/hdruk/hdruk_definitions.parquet",
    "OPEN_CODELISTS": "_external_definitions/data/open_codelists_compiled/open_codelists_definitions.parquet",
    "BSA_BNF_SNOMED_MAPPINGS": "_external_definitions/data/bnf_to_snomed/processed_bnf_data.parquet"
}


class ExternalDefinitionLoader:
    """
    Generic loader to handle different external definition sources
    """

    def __init__(self, config):
        self.config = config
        self.database = config["definition_library"]["database"]
        self.schema = config["definition_library"]["schema"]

    def load_from_parquet(self, session, parquet_path: str, table_name: str):
        """
        Load definitions from parquet file to Snowflake table
        """
        try:
            df = pd.read_parquet(parquet_path)
            print(f"Loaded {table_name} definitions from parquet file - {len(df)} rows")

            load_definitions_to_snowflake(
                session=session,
                df=df,
                table_name=table_name,
                database=self.database,
                schema=self.schema
            )
            print(f"{table_name} definitions uploaded to Snowflake")
        except Exception as e:
            print(f"Error loading {table_name}: {str(e)}")
            print(f"Skipping {table_name}")

    def load_all_external_definitions(self, session):
        """
        Load all external definition sources
        """
        for table_name, parquet_path in EXTERNAL_DEFINITION_SOURCES.items():
            if os.path.exists(parquet_path):
                self.load_from_parquet(session, parquet_path, table_name)
            else:
                print(f"Skipping {table_name} as parquet file not found: {parquet_path}")


### MEASUREMENT CONFIGURATION

def create_measurement_configs_tables_local(session, config):
    """
    Create tables for measurement configurations in Snowflake (local version)
    """
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


def load_measurement_configs_into_tables_local(session, config):
    """Load measurement configurations into tables (local version)"""

    # Load measurement configs list
    config_list = []
    icb_name = config['icb_name']
    measurement_dir = f"data/measurements/{icb_name}"

    if os.path.exists(measurement_dir):
        config_list = [f for f in os.listdir(measurement_dir)
                       if f.endswith(".json") and f.startswith("standard_")]

    if not config_list:
        print("No measurement config files found")
        return

    for config_file in config_list:
        try:
            file_path = os.path.join(measurement_dir, config_file)
            measurement_config = load_measurement_config_from_json(file_path)

            standard_units, unit_mappings, unit_conversions, value_bounds = measurement_config.to_dataframes()

            # Delete all existing entries for this definition
            queries = [
                f"""DELETE FROM {config["measurement_configs"]["database"]}.
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
                WHERE DEFINITION_NAME = '{measurement_config.definition_name}'"""
            ]

            for query in queries:
                session.sql(query).collect()

            # Insert new measurement config entry
            session.sql(f"""INSERT INTO {config["measurement_configs"]["database"]}.
                {config["measurement_configs"]["schema"]}.MEASUREMENT_CONFIGS
                (DEFINITION_ID, DEFINITION_NAME, CONFIG_ID, CONFIG_VERSION)
                VALUES (
                    '{measurement_config.definition_id}',
                    '{measurement_config.definition_name}',
                    '{measurement_config.standard_measurement_config_id}',
                    '{measurement_config.standard_measurement_config_version}'
                )""").collect()

            # Insert dataframes if not empty
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

        except Exception as e:
            print(f"Error loading measurement config {config_file}: {e}")

### UNIONED VIEW

def create_definitionstore_view(session, database, schema, external_tables):
    """
    Creates unified view of all definition tables with DBID mappings
    """
    # Always include core tables
    CORE_TABLES = ["AIC_DEFINITIONS", "ICB_DEFINITIONS"]

    # Add external tables from config
    ALL_TABLES = CORE_TABLES + list(external_tables.keys())

    view_sql = f"""
    CREATE OR REPLACE VIEW {database}.{schema}.DEFINITIONSTORE AS
    WITH definition_union AS (
        {
        " UNION ALL ".join(
            f"SELECT *, '{table}' AS SOURCE_TABLE FROM {database}.{schema}.{table} WHERE CODE IS NOT NULL"
            for table in ALL_TABLES
        )
    }
    )
    SELECT
        p.*,
        c.DBID,
        CASE c.MAPPING_TYPE
            WHEN 'Core SNOMED' THEN c.DBID
            WHEN 'Non Core Mapped to SNOMED' THEN cm.CORE
            ELSE NULL
        END as CORE_CONCEPT_ID
    FROM definition_union p
    LEFT JOIN PROD_DWH.ANALYST_PRIMARY_CARE.CONCEPT c
        ON p.CODE = c.CODE
        AND p.VOCABULARY = c.SCHEME_NAME
    LEFT JOIN PROD_DWH.ANALYST_PRIMARY_CARE.CONCEPT_MAP cm
        ON c.DBID = cm.LEGACY
        AND c.MAPPING_TYPE = 'Non Core Mapped to SNOMED'
    """
    session.sql(view_sql).collect()
    print("Created DEFINITIONSTORE view with DBID mappings")