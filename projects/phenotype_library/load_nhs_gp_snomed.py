from datetime import datetime

import pandas as pd
from base.load_tables import load_phenotypes_to_snowflake
from base.phenotype import Code, Codelist, Phenotype, PhenotypeSource, VocabularyType
from dotenv import load_dotenv

from phmlondon.onto_utils import FHIRTerminologyClient
from phmlondon.snow_utils import SnowflakeConnection

################################################################################################
# SNOMED Monoliths for retrieval
refsets = [
    # {
    #     'name': 'UK SNOMED Diagnoses 2023-07 Experimental',
    #     'url': 'http://snomed.info/xsct/999000011000230102/version/20230705?fhir_vs=refset'
    # },
    {
        "name": "UK SNOMED Diagnoses 2025-03 Experimental",
        "url": "http://snomed.info/xsct/83821000000107/version/20250115?fhir_vs=refset",
    }
]
################################################################################################


def transform_fhir_to_phenotypes(refsets_df, version_date):
    """
    Transform FHIR refsets dataframe into list of Phenotype objects
    """
    phenotypes = []
    current_datetime = datetime.now()

    # group by refset to create phenotypes
    for refset_code, refset_group in refsets_df.groupby("refset_code"):
        # sample row for refset level info
        first_row = refset_group.iloc[0]

        # get refset name based on naming structure
        try:
            parsed_name = first_row["refset_name"].split("-")[2].strip().capitalize()
        except IndexError:
            print("Warning: Could not parse refset_name")
            parsed_name = first_row["refset_name"]  # fallback to original name

        # create list of code objects
        codes = [
            Code(
                code=row["concept_code"],
                code_description=row["concept_name"],
                code_vocabulary=VocabularyType.SNOMED,  # we define this as it is not a field in the
                # source data
            )
            for _, row in refset_group.iterrows()
        ]

        # Create Codelist object
        codelist = Codelist(
            codelist_id=refset_code,
            codelist_name=parsed_name,
            codelist_vocabulary=VocabularyType.SNOMED,
            codelist_version=first_row["url"],
            codes=codes,
        )

        # Create Phenotype object
        phenotype = Phenotype(
            phenotype_id=refset_code,
            phenotype_name=parsed_name,
            phenotype_version=first_row["url"],
            phenotype_source=PhenotypeSource.LONDON,
            codelists=[codelist],  # For NHS GP refsets, this will be a single codelist
            version_datetime=version_date,
            uploaded_datetime=current_datetime,
        )
        phenotypes.append(phenotype)

    return phenotypes


def retrieve_snomed_phenotypes(url: str) -> pd.DataFrame:
    """
    Retrieves phenotypes from OneLondon Terminology server and transforms to a DataFrame.
    Args:
        url (str):
            url to retrieve data from
    Returns:
        DataFrame containing phenotype data
    """
    try:
        fhir_client = FHIRTerminologyClient(endpoint_type="authoring")
        refsets = fhir_client.retrieve_refsets_from_megalith(
            url, name_filter="General practice data extraction"
        )

        # extract version date from URL
        version_string = url.split("version/")[1].split("?")[0]
        version_date = datetime.strptime(version_string, "%Y%m%d")

        # transform to phenotypes
        phenotypes = transform_fhir_to_phenotypes(refsets, version_date)
        print(f"Created {len(phenotypes)} phenotype objects")

        # create combined dataframe from all phenotypes
        df = pd.concat([p.to_dataframe() for p in phenotypes], ignore_index=True)
        df.columns = df.columns.str.upper()

        print("DataFrame preview:")
        print(df.head())

        return df

    except Exception as e:
        print(f"Error retrieving phenotypes: {e}")
        raise e


def main():
    load_dotenv()

    snowsesh = SnowflakeConnection()
    snowsesh.use_database("INTELLIGENCE_DEV")
    snowsesh.use_schema("AI_CENTRE_PHENOTYPE_LIBRARY")

    try:
        for refset in refsets:
            print(f"Processing {refset['name']}...")

            # Retrieve phenotypes
            df = retrieve_snomed_phenotypes(refset["url"])

            # Load to Snowflake
            load_phenotypes_to_snowflake(
                snowsesh=snowsesh, df=df, table_name="NHS_GP_SNOMED_REFSETS"
            )

            print(f"Completed processing {refset['name']}")

    except Exception as e:
        print(f"Error in main process: {e}")
        raise e
    finally:
        snowsesh.session.close()


if __name__ == "__main__":
    main()
