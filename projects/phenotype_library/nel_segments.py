from datetime import datetime

import pandas as pd
from base.load_tables import load_phenotypes_to_snowflake
from base.phenotype import Code, Codelist, Phenotype, PhenotypeSource, VocabularyType
from dotenv import load_dotenv

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
        CONDITION_UPPER as PHENOTYPE_NAME,
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


def transform_to_phenotype_model(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforms segmentation data into phenotype model
    Args:
        df (pd.DataFrame):
            Dataframe of NEL segments
    Returns:
        result_df (pd.DataFrame):
            Dataframe structured using phenotype objects
    """
    print("Sample data:")
    print(df.head())

    phenotypes = []
    current_datetime = datetime.now()

    for phenotype_name, group in df.groupby("phenotype_name"):
        print(f"Processing phenotype: {phenotype_name}")
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
                codelist_id=f"{phenotype_name}_{vocabulary.value}",
                codelist_name=f"{phenotype_name} {vocabulary.value}",
                codelist_vocabulary=vocabulary,
                codelist_version="1.0",
                codes=codes,
            )
            codelists.append(codelist)

        phenotype = Phenotype(
            phenotype_id=phenotype_name,
            phenotype_name=phenotype_name,
            phenotype_version="1.0",
            phenotype_source=PhenotypeSource.ICB_NEL,
            codelists=codelists,
            version_datetime=current_datetime,
            uploaded_datetime=current_datetime,
        )
        phenotypes.append(phenotype)

    # into dataframe
    result_df = pd.concat([p.to_dataframe() for p in phenotypes], ignore_index=True)
    result_df.columns = result_df.columns.str.upper()

    print("Output sample data:")
    print(result_df.head())

    return result_df


def main():
    load_dotenv()

    snowsesh = SnowflakeConnection()
    snowsesh.use_database("INTELLIGENCE_DEV")
    snowsesh.use_schema("AI_CENTRE_PHENOTYPE_LIBRARY")

    try:
        # Fetch source data
        print("Fetching source data...")
        source_df = fetch_segmentation_codes(snowsesh)

        # Transform to phenotype model
        print("Transforming to phenotype model...")
        phenotype_df = transform_to_phenotype_model(source_df)

        # Load to target table
        print("Loading to Snowflake...")
        load_phenotypes_to_snowflake(
            snowsesh=snowsesh, df=phenotype_df, table_name="NEL_SEGMENT_PHENOTYPES"
        )

        print("NEL segment phenotypes loaded successfully")

    except Exception as e:
        print(f"Error loading NEL segment phenotypes: {e}")
        raise e
    finally:
        snowsesh.session.close()


if __name__ == "__main__":
    main()
