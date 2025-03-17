from datetime import datetime

import pandas as pd
import git
import os
from pathlib import Path
from dotenv import load_dotenv
from loaders.base.scrape_open_codelists import return_version_id_from_open_codelist_url

from loaders.base.load_tables import load_phenotypes_to_snowflake
from loaders.base.phenotype import Phenotype
from phmlondon.snow_utils import SnowflakeConnection

phenotypes_to_load = {
    "https://www.opencodelists.org/codelist/opensafely/hypertension-snomed/2020-04-28/":
        "data/open_codelist_csvs/opensafely-hypertension-snomed-2020-04-28.csv",
    "https://www.opencodelists.org/codelist/nhsd-primary-care-domain-refsets/abpm_cod/20241205/":
        "data/open_codelist_csvs/nhsd-primary-care-domain-refsets-abpm_cod-20241205.csv",
    "https://www.opencodelists.org/codelist/opensafely/height-snomed/3b4a3891/":
        "data/open_codelist_csvs/opensafely-height-snomed-3b4a3891.csv",
    "https://www.opencodelists.org/codelist/opensafely/weight-snomed/5459abc6/":
        "data/open_codelist_csvs/opensafely-weight-snomed-5459abc6.csv",
    "https://www.opencodelists.org/codelist/opensafely/stroke-snomed/2020-04-21/#full-list":
        "data/open_codelist_csvs/opensafely-stroke-snomed-2020-04-21.csv",
    "https://www.opencodelists.org/codelist/opensafely/medication-reviews-all-types/69f99fda/":
        "data/open_codelist_csvs/opensafely-medication-reviews-all-types-69f99fda.csv",
    "https://www.opencodelists.org/codelist/nhsd-primary-care-domain-refsets/dmnontype1_cod/20241205/":
        "data/open_codelist_csvs/nhsd-primary-care-domain-refsets-dmnontype1_cod-20241205.csv",
}

def get_git_root(path: str) -> str:
    """
    Given a path, return the root directory of the Git repository

    Args:
        path (str): Path to a file within a Git repository
    Returns:
        str: Root directory of the Git repository
    """
    repo = git.Repo(path, search_parent_directories=True)
    return str(repo.working_tree_dir)

def open_codelists_url_and_csv_to_phenotype(url: str, csv_path: str) -> pd.DataFrame:
    """
    Given an OpenCodelists URL and a local CSV file path, retrieve the codelist metadata and transform the CSV into a
    Phenotype object, then return the Phenotype as a DataFrame

    Args:
        url (str): URL of the OpenCodelists codelist
        csv_path (str): Local path to the CSV file
    Returns:
        pd.DataFrame: DataFrame representation of the Phenotype
    """
    vocabulary, codelist_name, codelist_id, version_id, version_datetime = return_version_id_from_open_codelist_url(url)
    # print(vocabulary)
    # print(version_id)

    full_file_path = Path(get_git_root(os.getcwd())) / Path('projects/phenotype_library/') /  Path(csv_path)
    df_from_file = pd.read_csv(full_file_path)
    # print(df_from_file)

    df_to_create_phenotype = df_from_file.iloc[:, [0, 1]].set_axis(["code", "code_description"], axis=1)
    df_to_create_phenotype["vocabulary"] = vocabulary
    df_to_create_phenotype["codelist_id"] = codelist_id
    df_to_create_phenotype["codelist_name"] = codelist_name
    df_to_create_phenotype["codelist_version"] = version_id
    df_to_create_phenotype["phenotype_id"] = codelist_id
    df_to_create_phenotype["phenotype_name"] = codelist_name
    df_to_create_phenotype["phenotype_version"] = version_id
    df_to_create_phenotype["phenotype_source"] = "OPEN_CODELISTS"
    df_to_create_phenotype["version_datetime"] = version_datetime

    # print(df_to_create_phenotype.head())

    phenotype = Phenotype.from_dataframe(df_to_create_phenotype)
    phenotype.uploaded_datetime = datetime.now()

    df = phenotype.to_dataframe()
    df.columns = df.columns.str.upper()

    print(f"Retrieved and transformed phenotype {codelist_id}")
    print("DataFrame preview:")
    print(df.head())

    return df

def main():
    load_dotenv()

    snowsesh = SnowflakeConnection()
    snowsesh.use_database("INTELLIGENCE_DEV")
    snowsesh.use_schema("AI_CENTRE_PHENOTYPE_LIBRARY")

    try:
        for url, csv_path in phenotypes_to_load.items():
            df = open_codelists_url_and_csv_to_phenotype(url, csv_path)
            load_phenotypes_to_snowflake(snowsesh=snowsesh, df=df, table_name="OPEN_CODELISTS_PHENOTYPES")
            print(f"Completed processing phenotype {url}")
    except Exception as e:
        print(f"Failed to load phenotype {url}: {e}")
    finally:
        snowsesh.session.close()

if __name__ == "__main__":
    print("ERROR: This script should not be run directly.")
    print("Please run from update.py using the appropriate flag.")
