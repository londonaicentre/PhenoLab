"""
This script is triggered by deploy.sh to create tables and load definitions

It can also be run independently to push data to either a dev or prod ICB environment
Usage: python setup.py <connection_name> <environment>
Example: python setup.py nel_icb prod
"""
import sys

import pandas as pd
import yaml
from create_tables import create_definition_table, load_definitions_to_snowflake
from snowflake.snowpark import Session
from utils.config_utils import load_config
from utils.definition_interaction_utils import update_aic_definitions_table
from utils.measurement_interaction_utils import create_measurement_configs_tables, load_measurement_configs_into_tables


def create_definitionstore_view(session: Session, database: str, schema: str, external_tables: list, config: dict):
    """
    Creates unioned view of all definition tables with DBID mappings

    Args:
        session (Session): Snowflake session
        database (str): Database name
        schema (str): Schema name
        external_tables (dict): Dictionary of external tables with keys as table names and values as their
        config (dict): Configuration dictionary containing concept table references
    """
    # Always include core tables
    CORE_TABLES = ["AIC_DEFINITIONS", "ICB_DEFINITIONS"]

    # Add external tables from config
    ALL_TABLES = CORE_TABLES + external_tables

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
        c.DB_CONCEPT_ID as DBID,
        COALESCE(cm.TARGET_DB_CONCEPT_ID, c.DB_CONCEPT_ID) as CORE_CONCEPT_ID
    FROM definition_union p
    LEFT JOIN {config["gp_concept_table"]} c
        ON p.CODE = c.CONCEPT_CODE
        AND p.VOCABULARY = c.CONCEPT_VOCABULARY
    LEFT JOIN {config["gp_concept_map_table"]} cm
        ON c.DB_CONCEPT_ID = cm.SOURCE_DB_CONCEPT_ID
        AND c.DB_CONCEPT_ID_TYPE = cm.SOURCE_DB_CONCEPT_ID_TYPE
    """
    session.sql(view_sql).collect()
    print("Created DEFINITIONSTORE view with DBID mappings")

def setup_definition_tables(environment: str, connection_name: str):
    """
    Set up definition tables and load initial definitions into Snowflake.

    Args:
        environment (str): Environment to use (e.g., 'dev', 'prod')
        connection_name (str): Name of the Snowflake connection to use.
    """

    # Load config
    session = Session.builder.config("connection_name", connection_name).create()
    config = load_config(session=session, deploy_env=environment)

    # 1. AIC
    create_definition_table(
            session=session,
            database=config["definition_library"]["database"],
            schema=config["definition_library"]["schema"],
            table_name="AIC_DEFINITIONS"
        )
    update_aic_definitions_table(session=session, config=config)

    # 2. External definitions (HDRUK, OpenCodelists, Ontoserver SNOMED)
    with open("external_definitions.yml", "r") as f:
        external_definition_sources = yaml.safe_load(f)

    for table_name, ext_config in external_definition_sources.items():
        file_name = ext_config["file"]
        df = pd.read_parquet(file_name)
        print(f"Loaded {file_name} definitions from file - {len(df)} rows")
        load_definitions_to_snowflake(session=session, df=df, table_name=table_name,
            database=config["definition_library"]["database"], schema=config["definition_library"]["schema"])
        print(f"Loaded definitions into Snowflake table {table_name}")

    # 3. Create ICB_DEFINITIONS table if it doesn't exist (preserve user data)
    print("Creating ICB_DEFINITIONS table if it doesn't exist...")
    create_definition_table(
        session=session,
        database=config["definition_library"]["database"],
        schema=config["definition_library"]["schema"],
        table_name="ICB_DEFINITIONS"
    )

    # 4. Create DEFINITIONSTORE view - this doesn't strictly need to be run every time as it's a VIEW and not a table,
    # but I have left it in for clarity and to ensure it is always up to date with the latest external tables.
    print("Creating DEFINITIONSTORE view...")
    create_definitionstore_view(
        session=session,
        database=config["definition_library"]["database"],
        schema=config["definition_library"]["schema"],
        external_tables=list(external_definition_sources.keys()),
        config=config
    )

    # 5. Create measurement config tables
    create_measurement_configs_tables(session=session, config=config)
    load_measurement_configs_into_tables(session=session, config=config)

if __name__ == "__main__":
    if len(sys.argv) == 1:
        # keeping for now for backwards compatibility
        print("No arguments provided, using default: sel_icb dev")
        connection_name = 'sel_icb'
        environment = 'dev'
    elif len(sys.argv) == 3:
        connection_name = sys.argv[1]
        environment = sys.argv[2]

        if environment not in ['dev', 'prod']:
            print("Error: Environment must be 'dev' or 'prod'")
            sys.exit(1)
    else:
        print("Use directly with python setup.py <connection_name> <environment>")
        print("E.g. python setup.py sel_icb dev")
        sys.exit(1)

    setup_definition_tables(environment=environment, connection_name=connection_name)
