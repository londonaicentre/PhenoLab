"""
This standalone script should be manually run to set up the tables in a new Phenolab instance.
Can be called with environment and connection parameters.

Usage: python setup.py <environment> <connection_name>
Example: python setup.py prod nel_icb
"""
import sys

import yaml
from definition_library.create_definitionstore_view import create_definitionstore_view
from definition_library.loaders.create_tables import create_definition_table
from features_base.base_apc_concepts import BASE_SUS_APC_CONCEPTS_SQL
from snowflake.snowpark import Session


def run_setup(environment: str, connection_name: str):
    # build config filename based on target environment
    # (inputs as command line args or from bash script)
    config_file = f'{connection_name}_{environment}.yml'
    config_path = f"configs/{config_file}"

    print(f"Loading configuration from: {config_path}")

    with open(config_path, "r") as fid:
        config = yaml.safe_load(fid)

    session = Session.builder.config("connection_name", connection_name).create()

    # Create the definition library tables if they do not exist
    tables_to_create = [
        "AIC_DEFINITIONS",
        "BSA_BNF_SNOMED_MAPPINGS",
        "HDRUK_DEFINITIONS",
        "ICB_DEFINITIONS",
        "OPEN_CODELISTS",
        "NEL_SEGMENT_DEFINITIONS",
        "NHS_GP_SNOMED_REFSETS",
    ]
    for t in tables_to_create:
        create_definition_table(
            session=session,
            database=config["definition_library"]["database"],
            schema=config["definition_library"]["schema"],
            table_name=t)

    # Creates the definition store view which unifies the definition tables
    create_definitionstore_view(
        session=session,
        database=config["definition_library"]["database"],
        schema=config["definition_library"]["schema"])

    # Create the base SUS APC concepts feature
    session.sql(f"""
        CREATE OR REPLACE TABLE {config['feature_store']['database']}.
        {config['feature_store']['schema']}.BASE_APC_CONCEPTS AS
        {BASE_SUS_APC_CONCEPTS_SQL}
        """).collect()
    print("Base SUS APC concepts feature created successfully.")


if __name__ == "__main__":
    if len(sys.argv) == 1:
        # keeping for now for backwards compatibility
        print("No arguments provided, using default: prod nel_icb")
        environment = 'prod'
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

    run_setup(environment, connection_name)
