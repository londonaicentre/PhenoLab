from datetime import datetime

import pandas as pd
from base.load_tables import load_phenotypes_to_snowflake
from base.phenotype import Phenotype
from dotenv import load_dotenv
from scrape_open_codelists import return_version_id_from_open_codelist_url

from phmlondon.snow_utils import SnowflakeConnection

phenotypes_to_load = {
    "https://www.opencodelists.org/codelist/opensafely/hypertension-snomed/2020-04-28/":
        "data/open_codelist_csvs/opensafely-hypertension-snomed-2020-04-28.csv",
    "https://www.opencodelists.org/codelist/nhsd-primary-care-domain-refsets/abpm_cod/20241205/":
        "data/open_codelist_csvs/nhsd-primary-care-domain-refsets-abpm_cod-20241205.csv",
    "https://www.opencodelists.org/codelist/opensafely/height-snomed/3b4a3891/":
        "data/open_codelist_csvs/opensafely-height-snomed-3b4a3891.csv",
    "https://www.opencodelists.org/codelist/opensafely/weight-snomed/5459abc6/":
        "data/open_codelist_csvs/opensafely-weight-snomed-5459abc6.csv"
}

def open_codelists_url_and_csv_to_phenotype(url: str, csv_path: str) -> pd.DataFrame:
    vocabulary, codelist_name, codelist_id, version_id, version_datetime = return_version_id_from_open_codelist_url(url)
    # print(vocabulary)
    # print(version_id)

    df_from_file = pd.read_csv(csv_path)
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
    main()
