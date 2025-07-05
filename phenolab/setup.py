"""
This script is triggered by deploy.sh to create tables and load definitions

It can also be run independently to push data to either a dev or prod ICB environment
Usage: python setup.py <environment> <connection_name>
Example: python setup.py prod nel_icb
"""
import sys

import yaml
from create_tables import create_definition_table
from loaders import (
    EXTERNAL_DEFINITION_SOURCES,
    ExternalDefinitionLoader,
    create_definitionstore_view,
    create_measurement_configs_tables_local,
    load_measurement_configs_into_tables_local,
    update_aic_definitions_local,
)
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

    # Create ICB_DEFINITIONS table if it doesn't exist (preserve user data)
    print("Creating ICB_DEFINITIONS table if it doesn't exist...")
    create_definition_table(
        session=session,
        database=config["definition_library"]["database"],
        schema=config["definition_library"]["schema"],
        table_name="ICB_DEFINITIONS"
    )

    # AI Definitions
    print("Loading AIC definitions from JSON files...")
    update_aic_definitions_local(session, config)

    # External Definitions
    print("Loading external definitions...")
    external_loader = ExternalDefinitionLoader(config)
    external_loader.load_all_external_definitions(session)

    # Create DEFINITIONSTORE
    print("Creating DEFINITIONSTORE view...")
    create_definitionstore_view(
        session=session,
        database=config["definition_library"]["database"],
        schema=config["definition_library"]["schema"],
        external_tables=EXTERNAL_DEFINITION_SOURCES)

    # Load measurement configs
    print("Creating measurement config tables...")
    create_measurement_configs_tables_local(session, config)

    print("Loading measurement configurations...")
    load_measurement_configs_into_tables_local(session, config)



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

    run_setup(environment, connection_name)
