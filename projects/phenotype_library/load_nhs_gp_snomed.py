from dotenv import load_dotenv
from phmlondon.onto_utils import FHIRTerminologyClient
from phmlondon.snow_utils import SnowflakeConnection
from src.phenotype import Phenotype, VocabularyType, PhenotypeSource
from datetime import datetime

def transform_fhir_to_phenotypes(refsets_df, version_date):
    """
    Transform FHIR refsets dataframe into list of Phenotype objects
    """
    phenotypes = []
    current_datetime = datetime.now()

    for _, row in refsets_df.iterrows():

        # get refset name based on naming structure
        try:
            parsed_name = row['refset_name'].split('-')[2].strip()
        except IndexError:
            print(f"Warning: Could not parse refset_name")
            parsed_name = row['refset_name']  # fallback to original name

        phenotype = Phenotype(
            concept_code=row['concept_code'],
            concept_name=row['concept_name'],
            vocabulary=VocabularyType.SNOMED,
            codelist_id=row['refset_code'],
            codelist_name=parsed_name,
            codelist_version=row['url'],
            phenotype_id=row['refset_code'],
            phenotype_name=row['refset_name'],
            phenotype_version=row['url'],
            phenotype_source=PhenotypeSource.LONDON,
            omop_concept_id=None,
            version_datetime=version_date,
            uploaded_datetime=current_datetime
        )
        phenotypes.append(phenotype)

    return phenotypes

def retrieve_and_load_phenotypes(snowsesh, url):
    """
    Retrieves FHIR data, transforms to phenotypes, and loads to Snowflake
    """
    try:
        fhir_client = FHIRTerminologyClient(endpoint_type='authoring')
        refsets = fhir_client.retrieve_refsets_from_megalith(
            url,
            name_filter="General practice data extraction")

        # extract version date from URL
        version_string = url.split('version/')[1].split('?')[0]
        version_date = datetime.strptime(version_string, '%Y%m%d')

        # transform to phenotypes
        phenotypes = transform_fhir_to_phenotypes(refsets, version_date)
        print(f"Created {len(phenotypes)} phenotype objects")

        # create dataframe
        df = Phenotype.to_dataframe(phenotypes)
        print("DataFrame preview before load:")
        print(df.head())

        snowsesh.load_dataframe_to_table(
            df=df,
            table_name="NHS_GP_SNOMED_REFSETS",
            mode="append"
        )
        print("Phenotypes loaded to database")

    except Exception as e:
        print(f"Error processing phenotypes: {e}")
        raise e

def main():
    load_dotenv()

    snowsesh = SnowflakeConnection()
    snowsesh.use_database("INTELLIGENCE_DEV")
    snowsesh.use_schema("AI_CENTRE_PHENOTYPE_LIBRARY")

    try:
        # always create table if not exist
        snowsesh.execute_sql_file('sql/ddl_nhs_gp_snomed.sql')
        print("Table structure ensured")

        # process different SNOMED refsets
        refsets = [
            {
                'name': 'UK SNOMED Diagnoses 2023-07',
                'url': 'http://snomed.info/xsct/999000011000230102/version/20230705?fhir_vs=refset'
            }
        ]

        for refset in refsets:
            print(f"Processing {refset['name']}...")
            retrieve_and_load_phenotypes(snowsesh, refset['url'])
            print(f"Completed processing {refset['name']}")

    except Exception as e:
        print(f"Error in main process: {e}")
        raise e
    finally:
        snowsesh.session.close()

if __name__ == "__main__":
    main()