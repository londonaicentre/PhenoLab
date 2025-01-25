from dotenv import load_dotenv
from phmlondon.snow_utils import SnowflakeConnection
from phmlondon.hdruk_api import HDRUKLibraryClient
from src.phenotype import Phenotype
from datetime import datetime
import pandas as pd
from src.load_tables import load_phenotypes_to_snowflake

#######################################################
# Active phenotypes for retrieval from HDR-UK library #
phenotype_list = [
    ("PH152", 304),  # Diabetes, any
]
#######################################################

def retrieve_hdruk_phenotypes(
        phenotype_id: str,
        version_id: int
        ) -> pd.DataFrame:
    """
    Retrieves phenotype data from HDRUK API and returns as a DataFrame.
    Args:
        phenotype_id (str):
            ID of the phenotype
        version_id (int):
            Version of the phenotype
    Returns:
        DataFrame containing phenotype data
    """
    try:
        print(f"Processing phenotype {phenotype_id} version {version_id}...")

        hdr_client = HDRUKLibraryClient()

        # Get codelist data from API
        codelist_df = hdr_client.get_phenotype_codelist(
            phenotype_id=phenotype_id,
            version_id=version_id,
            output_format="db"
        )

        # Transform to phenotype object
        phenotype = Phenotype.from_dataframe(codelist_df)
        phenotype.uploaded_datetime = datetime.now()

        # Convert to pandas df
        df = phenotype.to_dataframe()
        df.columns = df.columns.str.upper()

        print(f"Retrieved and transformed phenotype {phenotype_id}")
        print("DataFrame preview:")
        print(df.head())

        return df

    except Exception as e:
        print(f"Error retrieving phenotype {phenotype_id}: {e}")
        raise e

def main():
    load_dotenv()

    snowsesh = SnowflakeConnection()
    snowsesh.use_database("INTELLIGENCE_DEV")
    snowsesh.use_schema("AI_CENTRE_PHENOTYPE_LIBRARY")

    try:
        for phenotype_id, version_id in phenotype_list:
            df = retrieve_hdruk_phenotypes(phenotype_id, version_id)

            load_phenotypes_to_snowflake(
                snowsesh=snowsesh,
                df=df,
                table_name="HDRUK_PHENOTYPES"
            )
            print(f"Completed processing phenotype {phenotype_id}")

    except Exception as e:
        print(f"Failed to load phenotype {phenotype_id}: {e}")
        raise e
    finally:
        snowsesh.session.close()

if __name__ == "__main__":
    main()