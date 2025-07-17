## prevents load from failing
import sys

## Must be run from update.py
from datetime import datetime

import pandas as pd
from dotenv import load_dotenv

from utils.definition import (
    Code,
    Codelist,
    Definition,
    DefinitionSource,
    VocabularyType,
)
from loaders.base.load_tables import load_definitions_to_snowflake
from phmlondon.snow_utils import SnowflakeConnection


def fetch_segmentation_codes(snowsesh) -> pd.DataFrame:
    """
    Fetches segmentation codes from source table
    Args:
        snowsesh:
            Snowflake session
    """
    query = """
    SELECT DISTINCT
        CONDITION_UPPER as DEFINITION_NAME,
        CODE as CODE,
        SCHEME as VOCABULARY,
        DESCRIPTION as CODE_DESCRIPTION
    FROM INTELLIGENCE_DEV.PHM_SEGMENTATION.SEGMENTATION_CODES
    WHERE SCHEME IN ('SNOMED', 'ICD10')
    ORDER BY CONDITION_UPPER
    """
    df = snowsesh.execute_query_to_df(query)
    # Convert column names to lowercase for consistency
    df.columns = df.columns.str.lower()
    return df


def transform_to_definition_model(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforms segmentation data into definition model
    Args:
        df (pd.DataFrame):
            Dataframe of NEL segments
    Returns:
        result_df (pd.DataFrame):
            Dataframe structured using definition objects
    """
    print("Sample data:")
    print(df.head())

    definitions = []
    current_datetime = datetime.now()

    for definition_name, group in df.groupby("definition_name"):
        print(f"Processing definition: {definition_name}")
        # group by vocabulary for codelists
        codelists = []
        for vocab, vocab_group in group.groupby("vocabulary"):
            print(f"Processing vocabulary: {vocab}")
            vocabulary = VocabularyType.SNOMED if vocab == "SNOMED" else VocabularyType.ICD10

            codes = [
                Code(
                    code=row["code"],
                    code_description=row["code_description"],
                    code_vocabulary=vocabulary,
                )
                for _, row in vocab_group.iterrows()
            ]

            codelist = Codelist(
                codelist_id=f"{definition_name}_{vocabulary.value}",
                codelist_name=f"{definition_name} {vocabulary.value}",
                codelist_vocabulary=vocabulary,
                codelist_version="1.0",
                codes=codes,
            )
            codelists.append(codelist)

        definition = Definition(
            definition_id=definition_name,
            definition_name=definition_name,
            definition_version="1.0",
            definition_source=DefinitionSource.ICB_NEL,
            codelists=codelists,
            version_datetime=current_datetime,
            uploaded_datetime=current_datetime,
        )
        definitions.append(definition)

    # into dataframe
    result_df = pd.concat([p.to_dataframe() for p in definitions], ignore_index=True)
    result_df.columns = result_df.columns.str.upper()

    print("Output sample data:")
    print(result_df.head())

    return result_df


def main():
    load_dotenv()

    snowsesh = SnowflakeConnection()
    snowsesh.use_database("INTELLIGENCE_DEV")
    snowsesh.use_schema("AI_CENTRE_DEFINITION_LIBRARY")

    try:
        # Fetch source data
        print("Fetching source data...")
        source_df = fetch_segmentation_codes(snowsesh)

        # Transform to definition model
        print("Transforming to definition model...")
        definition_df = transform_to_definition_model(source_df)

        # Load to target table
        print("Loading to Snowflake...")
        load_definitions_to_snowflake(
            snowsesh=snowsesh, df=definition_df, table_name="NEL_SEGMENT_DEFINITIONS"
        )

        print("NEL segment definitions loaded successfully")

    except Exception as e:
        print(f"Error loading NEL segment definitions: {e}")
        raise e
    finally:
        snowsesh.session.close()


if __name__ == "__main__":
    print("ERROR: This script should not be run directly.")
    print("Please run from update.py using the appropriate flag.")
    sys.exit(1)
