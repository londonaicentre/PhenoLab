"""
This script is triggered by deploy.sh to create tables and load definitions

It can also be run independently to push data to either a dev or prod ICB environment
Usage: python setup.py <environment> <connection_name>
Example: python setup.py prod nel_icb
"""
import sys

# import yaml
# from create_tables import create_definition_table
# from loaders import (
#     EXTERNAL_DEFINITION_SOURCES,
#     ExternalDefinitionLoader,
#     create_definitionstore_view,
#     create_measurement_configs_tables_local,
#     load_measurement_configs_into_tables_local,
#     update_aic_definitions_local,
# )
from snowflake.snowpark import Session



import pandas as pd

from create_tables import load_definitions_to_snowflake, create_definition_table
from utils.config_utils import load_config
from utils.definition_interaction_utils import update_aic_definitions_table
from utils.measurement_interaction_utils import create_measurement_configs_tables, load_measurement_configs_into_tables

def create_definitionstore_view(session: Session, database: str, schema: str, external_tables: list):
    """
    Creates unioned view of all definition tables with DBID mappings

    Args:
        session (Session): Snowflake session
        database (str): Database name
        schema (str): Schema name
        external_tables (dict): Dictionary of external tables with keys as table names and values as their 
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

def setup_definition_tables():
    """
    Set up definition tables and load initial definitions into Snowflake.
    """

    # Load config
    config = load_config()
    session = Session.builder.config("connection_name", config["icb_name"]).create()

    # 1. AIC
    create_definition_table( 
            session=session,
            database=config["definition_library"]["database"], 
            schema=config["definition_library"]["schema"],
            table_name="AI_CENTRE_DEFINITIONS"
        )
    update_aic_definitions_table(session=session, config=config)

    # 2. HDRUK
    df = pd.read_parquet('_external_definitions/data/hdruk/hdruk_definitions.parquet')
    print(f"Loaded HDRUK definitions from file - {len(df)} rows")
    load_definitions_to_snowflake(session=session, df=df, table_name="HDRUK_DEFINITIONS",
            database=config["definition_library"]["database"], schema=config["definition_library"]["schema"])

    # 3. NHS GP refsets
    #TODO

    # 4. Open Codelists
    df = pd.read_parquet("_external_definitions/data/open_codelists_compiled/open_codelists_definitions.parquet")
    print(f"Loaded Open Codelists definitions from file - {len(df)} rows")
    load_definitions_to_snowflake(session=session, df=df, table_name="OPEN_CODELISTS",
            database=config["definition_library"]["database"], schema=config["definition_library"]["schema"])

    # 5. BNF definitions
    df = pd.read_parquet("_external_definitions/data/bnf_to_snomed/processed_bnf_data.parquet")
    print(f"Loaded BSA BNF definitions from file - {len(df)} rows")
    load_definitions_to_snowflake(session=session, df=df, table_name="BSA_BNF_SNOMED_MAPPINGS",
            database=config["definition_library"]["database"], schema=config["definition_library"]["schema"])

    # 6. Create ICB_DEFINITIONS table if it doesn't exist (preserve user data)
    print("Creating ICB_DEFINITIONS table if it doesn't exist...")
    create_definition_table(
        session=session,
        database=config["definition_library"]["database"],
        schema=config["definition_library"]["schema"],
        table_name="ICB_DEFINITIONS"
    )

    # 7. ICB specific - need to do NEL segments
    # TODO

    # 8. Create DEFINITIONSTORE view - this doesn't strictly need to be run every time as it's a VIEW and not a table,
    # but I have left it in for clarity and to ensure it is always up to date with the latest external tables.
    print("Creating DEFINITIONSTORE view...")
    create_definitionstore_view(
        session=session,
        database=config["definition_library"]["database"],
        schema=config["definition_library"]["schema"],
        external_tables=EXTERNAL_DEFINITION_SOURCES)

    # 9. Create measurement config tables
    create_measurement_configs_tables(session=session, config=config)
    load_measurement_configs_into_tables(session=session, config=config)


# def run_setup(environment: str, connection_name: str):
    # # build config filename based on target environment
    # # (inputs as command line args or from bash script)
    # config_file = f'{connection_name}_{environment}.yml'
    # config_path = f"configs/{config_file}"

    # print(f"Loading configuration from: {config_path}")

    # with open(config_path, "r") as fid:
    #     config = yaml.safe_load(fid)

    # session = Session.builder.config("connection_name", connection_name).create()



    # # AI Definitions
    # print("Loading AIC definitions from JSON files...")
    # update_aic_definitions_local(session, config)

    # # External Definitions
    # print("Loading external definitions...")
    # external_loader = ExternalDefinitionLoader(config)
    # external_loader.load_all_external_definitions(session)

    # Create DEFINITIONSTORE
    # print("Creating DEFINITIONSTORE view...")
    # create_definitionstore_view(
    #     session=session,
    #     database=config["definition_library"]["database"],
    #     schema=config["definition_library"]["schema"],
    #     external_tables=EXTERNAL_DEFINITION_SOURCES)

    # Load measurement configs
    # print("Creating measurement config tables...")
    # create_measurement_configs_tables_local(session, config)

    # print("Loading measurement configurations...")
    # load_measurement_configs_into_tables_local(session, config)



if __name__ == "__main__":
    if len(sys.argv) == 1:
        # for backwards compatibility
        print("No arguments provided, using default: prod nel_icb")
        environment = 'dev'
        connection_name = 'nel_icb'
    elif len(sys.argv) == 3:
        environment = sys.argv[1]
        connection_name = sys.argv[2]

        if environment not in ['dev', 'prod']:
            print("Error: Environment must be 'dev' or 'prod'")
            sys.exit(1)
    else:
        print("Use directly with python setup.py <environment> <connection_name>")
        print("E.g. python setup.py prod nel_icb")
        sys.exit(1)

    # run_setup()
    setup_definition_tables()
