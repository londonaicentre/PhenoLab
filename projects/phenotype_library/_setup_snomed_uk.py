from dotenv import load_dotenv
from phmlondon.onto_utils import FHIRTerminologyClient
from phmlondon.snow_utils import SnowflakeConnection

def retrieve_and_load_megalith(snowsesh, url):
    """
    Retrieves megalith data and loads it directly into a Snowflake temporary table
        snowsesh: active snowflake connection custom class
        url: url for megalith retrieval
    """
    try:
        fhir_client = FHIRTerminologyClient(endpoint_type='authoring')
        refsets = fhir_client.retrieve_refsets_from_megalith(url)
        
        refsets.columns = ('MEGALITH',
                         'URL',
                         'REFSET_NAME',
                         'REFSET_CODE',
                         'CONCEPT_NAME',
                         'CONCEPT_CODE')
        print("Refsets retrieved successfully")
        
        # Load to temp table
        snowsesh.load_dataframe_to_table(
            df=refsets,
            table_name="SNOMED_UK_MEGALITH_TEMP",
            mode="overwrite",
            table_type="temporary"
        )
        
    except Exception as e:
        print(f"Error while retrieving and loading refset as temp table: {e}")
        raise e
    
def create_diagnosis_reference(snowsesh, sql_path):
    """
    Creates a reference table for UK SNOMED diagnoses with DBID join 
        snowsesh: active snowflake connection class
        sql_path: path to the SQL file containing table definitions
    """
    try:
        snowsesh.execute_sql_file(sql_path)
        print("SNOMED ref tables created")
        
    except Exception as e:
        print(f"Error creating SNOMED ref tables: {e}")
        raise e

if __name__ == "__main__":
    load_dotenv()
    
    snowsesh = SnowflakeConnection()

    snowsesh.use_database("INTELLIGENCE_DEV")
    snowsesh.execute_query("CREATE SCHEMA IF NOT EXISTS AI_CENTRE_PHENOTYPE_LIBRARY")
    snowsesh.use_schema("AI_CENTRE_PHENOTYPE_LIBRARY")

    try:
        # retrieve and load megalith from ontoserver
        url = 'http://snomed.info/xsct/999000011000230102/version/20230705?fhir_vs=refset'

        retrieve_and_load_megalith(snowsesh, url)
        
        # create UK SNOMED diagnosis reference table 
        create_diagnosis_reference(snowsesh, 'sql/uk_snomed_diagnosis_ref.sql')
        
    except Exception as e:
        print(f"Error creating reference table: {e}")
        raise e
    finally:
        snowsesh.session.close()