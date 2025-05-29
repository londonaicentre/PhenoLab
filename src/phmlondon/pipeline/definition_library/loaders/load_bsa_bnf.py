## prevents load from failing
import sys

## Must be run from update.py
import zipfile
from datetime import datetime
from io import BytesIO

import pandas as pd
from dotenv import load_dotenv

from phmlondon.definition import Code, Codelist, Definition, DefinitionSource, VocabularyType
from loaders.base.load_tables import load_definitions_to_snowflake  # noqa: F811
from phmlondon.snow_utils import SnowflakeConnection


def transform_to_definition(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform BNF data into definition objects
    """
    definitions = []
    current_datetime = datetime.now()

    # gropu by paragraph = definition
    for (para_code, para_name), para_group in df.groupby(["BNF Paragraph Code", "BNF Paragraph"]):
        # group by subparagraph = codelist
        codelists = []
        for (subpara_code, subpara_name), subpara_group in para_group.groupby(
            ["BNF Subparagraph Code", "BNF Subparagraph"]
        ):
            # codes from chemical substances
            codes = [
                Code(
                    code=row["BNF Chemical Substance Code"],
                    code_description=row["BNF Chemical Substance"],
                    code_vocabulary=VocabularyType.BNF,
                )
                for _, row in subpara_group.iterrows()
            ]

            codelist = Codelist(
                codelist_id=str(subpara_code),
                codelist_name=subpara_name,
                codelist_vocabulary=VocabularyType.BNF,
                codelist_version="1.0",
                codes=codes,
            )
            codelists.append(codelist)

        definition = Definition(
            definition_id=str(para_code),
            definition_name=para_name,
            definition_version="1.0",
            definition_source=DefinitionSource.NHSBSA,
            codelists=codelists,
            version_datetime=current_datetime,
            uploaded_datetime=current_datetime,
        )
        definitions.append(definition)

    return pd.concat([p.to_dataframe() for p in definitions], ignore_index=True)


def main():
    load_dotenv()

    snowsesh = SnowflakeConnection()
    snowsesh.use_database("INTELLIGENCE_DEV")
    snowsesh.use_schema("AI_CENTRE_DEFINITION_LIBRARY")

    zip_name = "20241101_bsa_bnf.zip"
    with zipfile.ZipFile(f"loaders/data/bsa_bnf/{zip_name}", 'r') as zip_ref:
        csv_name = zip_ref.namelist()[0]
        with zip_ref.open(csv_name) as csv_file:
            df = pd.read_csv(BytesIO(csv_file.read()))

    definition_df = transform_to_definition(df)
    definition_df.columns = definition_df.columns.str.upper()

    # Excluding "dummy paragraph" definitions
    definition_df = definition_df[~definition_df["DEFINITION_NAME"].str.contains("DUMMY")]

    load_definitions_to_snowflake(
        snowsesh=snowsesh,
        df=definition_df,
        table_name="BSA_BNF_HIERARCHY"
    )

    snowsesh.session.close()


if __name__ == "__main__":
    print("ERROR: This script should not be run directly.")
    print("Please run from update.py using the appropriate flag.")
    sys.exit(1)
