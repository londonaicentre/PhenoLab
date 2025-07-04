"""
This standalone script should be manually run to set up the tables in a new Phenolab instance.
Can be called with environment and connection parameters.

Usage: python setup.py <environment> <connection_name>
Example: python setup.py prod nel_icb
"""
import os
import sys
from datetime import datetime
from typing import List, Tuple

import pandas as pd
import yaml
from definition_library.create_definitionstore_view import create_definitionstore_view
from definition_library.loaders.create_tables import create_definition_table, load_definitions_to_snowflake
from features_base.base_apc_concepts import BASE_SUS_APC_CONCEPTS_SQL
from snowflake.snowpark import Session
from utils.definition import Code, Codelist, Definition, DefinitionSource, VocabularyType
from utils.definition_interaction_utils import load_definitions_list_from_local_files


def process_definitions_for_upload(definition_files: List[str], session, config) -> Tuple[pd.DataFrame, List[str], dict]:
    """
    Process all definition files and prepare them for upload to Snowflake (non-Streamlit version)
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


def update_aic_definitions_non_streamlit(session, config):
    """
    Update the AIC_DEFINITIONS table with new or updated definitions from local files (non-Streamlit version)
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


def load_hdruk_definitions(session, config):
    """Load HDR UK definitions from parquet file"""
    df = pd.read_parquet("definition_library/loaders/data/hdruk/hdruk_definitions.parquet")
    print(f"Loaded HDRUK definitions from parquet file - {len(df)} rows")
    load_definitions_to_snowflake(
        session=session,
        df=df,
        table_name="HDRUK_DEFINITIONS",
        database=config["definition_library"]["database"],
        schema=config["definition_library"]["schema"]
    )


def load_open_codelists_definitions(session, config):
    """Load Open Codelists definitions from parquet file"""
    df = pd.read_parquet("definition_library/loaders/data/open_codelists_compiled/open_codelists_definitions.parquet")
    print(f"Loaded OpenCodelists definitions from parquet file - {len(df)} rows")
    load_definitions_to_snowflake(
        session=session,
        df=df,
        table_name="OPEN_CODELISTS",
        database=config["definition_library"]["database"],
        schema=config["definition_library"]["schema"]
    )


def build_bnf_definitions(input_path: str) -> pd.DataFrame:
    """Build definitions from BNF chemical substances and SNOMED mappings"""
    joined_data = pd.read_parquet(f"{input_path}.parquet")

    definitions = []
    current_datetime = datetime.now()

    # group by chemical substance at definition level
    for (class_name, class_code), class_group in joined_data.groupby(
        ["BNF Subparagraph", "BNF Subparagraph Code"]
    ):
        # group by BNF Code at codelist level
        codelists = []
        for (bnf_code, chem_name), chem_name_group in class_group.groupby(["BNF Code", "BNF Chemical Substance"]):
            # create SNOMED codes for each mapping
            codes = [
                Code(
                    code=str(int(row["SNOMED Code"])),
                    code_description=row["DM+D: Product Description"],
                    code_vocabulary=VocabularyType.SNOMED,
                )
                for _, row in chem_name_group.iterrows()
            ]

            codelist = Codelist(
                codelist_id=bnf_code,
                codelist_name=chem_name,
                codelist_vocabulary=VocabularyType.SNOMED,
                codelist_version="1.0",
                codes=codes,
            )
            codelists.append(codelist)

        definition = Definition(
            definition_id=class_code,
            definition_name=class_name,
            definition_version="1.0",
            definition_source=DefinitionSource.NHSBSA,
            codelists=codelists,
            version_datetime=current_datetime,
            uploaded_datetime=current_datetime,
        )
        definitions.append(definition)

    return pd.concat([p.to_dataframe() for p in definitions], ignore_index=True)


def load_bnf_definitions(session, config):
    """Load BNF definitions from processed data"""
    definition_df = build_bnf_definitions(input_path="definition_library/loaders/data/bnf_to_snomed/processed_bnf_data")
    print("BNF definitions built")
    definition_df.columns = definition_df.columns.str.upper()

    # Excluding "dummy chemical" definitions
    definition_df = definition_df[~definition_df["DEFINITION_NAME"].str.contains("DUMMY")]

    load_definitions_to_snowflake(
        session=session,
        df=definition_df,
        table_name="BSA_BNF_SNOMED_MAPPINGS",
        database=config["definition_library"]["database"],
        schema=config["definition_library"]["schema"]
    )
    print("BNF definitions uploaded to snowflake")


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

    # Load AIC definitions from JSON files
    print("Loading AIC definitions from JSON files...")
    update_aic_definitions_non_streamlit(session, config)

    # Load external definitions
    print("Loading HDR UK definitions...")
    load_hdruk_definitions(session, config)

    print("Loading Open Codelists definitions...")
    load_open_codelists_definitions(session, config)

    print("Loading BNF definitions...")
    load_bnf_definitions(session, config)

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
        print("No arguments, using default: prod nel_icb")
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
