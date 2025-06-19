from datetime import datetime
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

from snowflake.snowpark import Session
from phmlondon.definition import Code, Codelist, Definition, DefinitionSource, VocabularyType
from phmlondon.snow_utils import SnowflakeConnection
from definition_library.loaders.create_tables import load_definitions_to_snowflake


def process_snomed_mappings(xlsx_files):
    """
    Combine and deduplicate mapping files
    """
    dfs = [pd.read_excel(f) for f in xlsx_files]
    combined = pd.concat(dfs)
    return combined.drop_duplicates(subset=["BNF Code", "SNOMED Code"])


def build_definitions(file_path, mapping_files):
    """
    Build definitions from BNF chemical substances and SNOMED mappings
    """
    with open(file_path) as csv_file:
        bnf_df = pd.read_csv(csv_file, 
            usecols=["BNF Chemical Substance", 
                     "BNF Subparagraph Code", 
                     "BNF Subparagraph", 
                     "BNF Chemical Substance Code"])
    
    chemical_substances = bnf_df[
        ["BNF Chemical Substance", "BNF Subparagraph Code", "BNF Subparagraph", "BNF Chemical Substance Code"]
    ].drop_duplicates()

    print("BNF dataframe created!")

    # process different years of BNF SNOMED mapping files
    mappings = process_snomed_mappings(mapping_files)

    mappings["Chemical Substance Code"] = mappings["BNF Code"].str[:9]

    # join mappings with BNF chemical substances
    joined_data = pd.merge(
        mappings,
        chemical_substances,
        left_on="Chemical Substance Code",
        right_on="BNF Chemical Substance Code",
    )
    joined_data = joined_data.dropna(subset=["SNOMED Code"])
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


def retrieve_bnf_definitions_and_add_to_snowflake(session: Session, database: str = "INTELLIGENCE_DEV",
        schema: str = "AI_CENTRE_DEFINITION_LIBRARY"):
                                                  
    mapping_files = Path("definition_library/loaders/data/bnf_to_snomed/").glob("*.xlsx")
    definition_df = build_definitions(
        "definition_library/loaders/data/bsa_bnf/20241101_1730476037387_BNF_Code_Information.csv", mapping_files)
    print("Definitions built")
    definition_df.columns = definition_df.columns.str.upper()
    print("success in naming!")

    # Excluding "dummy chemical" definitions
    definition_df = definition_df[~definition_df["DEFINITION_NAME"].str.contains("DUMMY")]

    load_definitions_to_snowflake(
        session=session, df=definition_df, table_name="BSA_BNF_SNOMED_MAPPINGS", database=database, schema=schema
    )
    print("uploaded to snowflake!")

if __name__ == "__main__":
    load_dotenv(override=True)
    conn = SnowflakeConnection()
    retrieve_bnf_definitions_and_add_to_snowflake(session=conn.session)