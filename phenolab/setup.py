"""
This standalone script should be manually run to set up the tables in a new Phenolab instance. Specify a config file 
that gives the database and schema of where to create the tables
"""
import yaml

from snowflake.snowpark import Session

from definition_library.loaders.create_tables import create_definition_table
from definition_library.create_definitionstore_view import create_definitionstore_view

def run_setup(config_file: str):
    with open(f"configs/{config_file}", "r") as fid:
        config = yaml.safe_load(fid)

    session = Session.builder.config("connection_name", "nel_icb").create()

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

if __name__ == "__main__":
    config_file = 'nel_icb_dev.yml'
    run_setup(config_file)