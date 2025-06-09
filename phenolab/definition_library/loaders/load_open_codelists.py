from datetime import datetime

import pandas as pd
import git
from dotenv import load_dotenv
from snowflake.snowpark import Session
from definition_library.loaders.scrape_open_codelists import return_version_id_from_open_codelist_url
from definition_library.loaders.create_tables import load_definitions_to_snowflake
from phmlondon.definition import Definition
from phmlondon.snow_utils import SnowflakeConnection

definitions_to_load = {
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
    "https://www.opencodelists.org/codelist/opensafely/symptoms-dizzy/5c7be00c/":
        "data/open_codelist_csvs/opensafely-symptoms-dizzy-5c7be00c.csv",
    "https://www.opencodelists.org/codelist/nhsd-primary-care-domain-refsets/breathlessness-codes/20241205/":
        "data/open_codelist_csvs/nhsd-primary-care-domain-refsets-breathlessness-codes-20241205.csv",
    "https://www.opencodelists.org/codelist/nhsd-primary-care-domain-refsets/memclin2_cod/20241205/":
         "data/open_codelist_csvs/nhsd-primary-care-domain-refsets-memclin2_cod-20241205.csv",
    "https://www.opencodelists.org/codelist/nhsd-primary-care-domain-refsets/frc_cod/20241205/":
        "data/open_codelist_csvs/nhsd-primary-care-domain-refsets-frc_cod-20241205.csv",
    "https://www.opencodelists.org/codelist/opensafely/symptoms-sleep-disturbance/59c92016/":
        "data/open_codelist_csvs/opensafely-symptoms-sleep-disturbance-59c92016.csv",
    "https://www.opencodelists.org/codelist/nhsd-primary-care-domain-refsets/ff_cod/20241205/":
        "data/open_codelist_csvs/nhsd-primary-care-domain-refsets-ff_cod-20241205.csv",
    "https://www.opencodelists.org/codelist/nhsd-primary-care-domain-refsets/syncope-or-dizziness-codes/20241205/":
        "data/open_codelist_csvs/nhsd-primary-care-domain-refsets-syncope-or-dizziness-codes-20241205.csv",
    "https://www.opencodelists.org/codelist/nhsd-primary-care-domain-refsets/peptic-ulceration-codes/20241205/":
        "data/open_codelist_csvs/nhsd-primary-care-domain-refsets-peptic-ulceration-codes-20241205.csv",
    "https://www.opencodelists.org/codelist/opensafely/symptoms-mobility-impairment/62a81387/":
        "data/open_codelist_csvs/opensafely-symptoms-mobility-impairment-62a81387.csv",
    "https://www.opencodelists.org/codelist/nhsd-primary-care-domain-refsets/thy_cod/20241205/":
        "data/open_codelist_csvs/nhsd-primary-care-domain-refsets-thy_cod-20241205.csv"

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

def open_codelists_url_and_csv_to_definition(url: str, csv_path: str) -> pd.DataFrame:
    """
    Given an OpenCodelists URL and a local CSV file path, retrieve the codelist metadata and transform the CSV into a
    Definition object, then return the Definition as a DataFrame

    Args:
        url (str): URL of the OpenCodelists codelist
        csv_path (str): Local path to the CSV file
    Returns:
        pd.DataFrame: DataFrame representation of the Definition
    """
    vocabulary, codelist_name, codelist_id, version_id, version_datetime = return_version_id_from_open_codelist_url(url)
    # print(vocabulary)
    # print(version_id)

    df_from_file = pd.read_csv('definition_library/loaders/' + csv_path)
    # print(df_from_file)

    df_to_create_definition = df_from_file.iloc[:, [0, 1]].set_axis(["code", "code_description"], axis=1)
    df_to_create_definition["vocabulary"] = vocabulary
    df_to_create_definition["codelist_id"] = codelist_id
    df_to_create_definition["codelist_name"] = codelist_name
    df_to_create_definition["codelist_version"] = version_id
    df_to_create_definition["definition_id"] = codelist_id
    df_to_create_definition["definition_name"] = codelist_name
    df_to_create_definition["definition_version"] = version_id
    df_to_create_definition["definition_source"] = "OPEN_CODELISTS"
    df_to_create_definition["version_datetime"] = version_datetime

    # print(df_to_create_definition.head())

    definition = Definition.from_dataframe(df_to_create_definition)
    definition.uploaded_datetime = datetime.now()

    # df = definition.to_dataframe()
    # df.columns = df.columns.str.upper()

    print(f"Retrieved and transformed definition {codelist_id}")
    # print("DataFrame preview:")
    # print(df.head())

    return definition.aslist

def retreive_open_codelists_definitions_from_list(definition_list: list) -> pd.DataFrame:
    """
    Takes a list of OpenCodelists URLs and their corresponding CSV paths, retrieves the definitions, and returns a
    DataFrame with all definitions represented.

    Args:
        definition_list (list): List of tuples containing OpenCodelists URL and CSV path
    Returns:
        pd.DataFrame: DataFrame containing all definitions
    """
    all_definitions = []
    for url, csv_path in definition_list.items():
        print(url)
        definition = open_codelists_url_and_csv_to_definition(url, csv_path)
        all_definitions.extend(definition)

    print('OpenCodelists definitions retrieved successfully')
    # combined_df = pd.concat(all_definitions, ignore_index=True)
    combined_df = pd.DataFrame(all_definitions)
    combined_df.columns = combined_df.columns.str.upper()

    return combined_df

def retrieve_open_codelists_definitions_and_add_to_snowflake(session: Session, database: str = "INTELLIGENCE_DEV",
        schema: str = "AI_CENTRE_DEFINITION_LIBRARY"):
    df = retreive_open_codelists_definitions_from_list(definitions_to_load)
    load_definitions_to_snowflake(session=session, df=df, table_name="OPEN_CODELISTS", 
        database=database, schema=schema)
    
if __name__ == "__main__":
    load_dotenv(override=True)
    conn = SnowflakeConnection()
    retrieve_open_codelists_definitions_and_add_to_snowflake(session=conn.session,
        database="INTELLIGENCE_DEV", schema="AI_CENTRE_DEFINITION_LIBRARY")
