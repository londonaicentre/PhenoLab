from datetime import datetime
from pathlib import Path

import pandas as pd
import streamlit as st
from dotenv import load_dotenv
from snowflake.snowpark import Session

from phmlondon.definition import Code, Codelist, Definition, DefinitionSource, VocabularyType
from phmlondon.snow_utils import SnowflakeConnection
from definition_library.loaders.create_tables import load_definitions_to_snowflake


def build_definitions(input_path: str) -> pd.DataFrame:
    """
    Build definitions from BNF chemical substances and SNOMED mappings
    """

    joined_data = pd.read_csv(f"{input_path}.csv")

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


def retrieve_bnf_definitions_and_add_to_snowflake(
        database: str = "INTELLIGENCE_DEV",
        schema: str = "AI_CENTRE_DEFINITION_LIBRARY"):

    definition_df = build_definitions(input_path="definition_library/loaders/data/bnf_to_snomed/processed_bnf_data")
    print("Definitions built")
    definition_df.columns = definition_df.columns.str.upper()
    print("success in naming!")

    # Excluding "dummy chemical" definitions
    definition_df = definition_df[~definition_df["DEFINITION_NAME"].str.contains("DUMMY")]

    load_definitions_to_snowflake(
        session=st.session_state.session, 
        df=definition_df, 
        table_name="BSA_BNF_SNOMED_MAPPINGS", 
        database=database, 
        schema=schema
    )
    print("uploaded to snowflake!")

if __name__ == "__main__":
    load_dotenv(override=True)
    conn = SnowflakeConnection()
    retrieve_bnf_definitions_and_add_to_snowflake(session=conn.session)