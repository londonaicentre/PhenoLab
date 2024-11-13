import os
import requests
import json
import pandas as pd
from dotenv import load_dotenv
from phmlondon.onto_utils import FHIRTerminologyClient
from phmlondon.snow_utils import SnowflakeConnection
import sys

def retrieve_megalith(url):
    """
    Retrieves megalith into df and prepares column naming for Snowflake
        url: url for megalith retrieval
    """
        
    fhir_client = FHIRTerminologyClient(endpoint_type='authoring')
    
    try:
        refsets = fhir_client.retrieve_refsets_from_megalith(url)  # dataframe
        refsets.columns = ('MEGALITH',
                           'URL',
                           'REFSET_NAME',
                           'REFSET_CODE',
                           'CONCEPT_NAME',
                           'CONCEPT_CODE')
        print("Refsets retrieved successfully")
        return refsets
    
    except Exception as e:
        print(f"Error retrieving refsets: {e}")
        sys.exit(1)

def load_megalith_to_snowflake(session, refsets_df):
    """
    Creates table and loads megalith data into Snowflake
        snowsesh: active snowflake connection class
        refsets_df: DataFrame containing megalith data
    """
    create_table_query = """
        CREATE TABLE IF NOT EXISTS SNOMED_UK_MEGALITH (
            MEGALITH VARCHAR(255),
            URL VARCHAR(255),
            REFSET_NAME VARCHAR(255),
            REFSET_CODE VARCHAR(255),
            CONCEPT_NAME VARCHAR(255),
            CONCEPT_CODE VARCHAR(255)
        )
    """
    
    try:
        snowsesh.execute_query(create_table_query)
        
        snowsesh.session.create_dataframe(refsets_df).write.save_as_table(
            "SNOMED_UK_MEGALITH",
            mode="overwrite"
        )

        print("Table written to Snowflake successfully.")
        
    except Exception as e:
        print(f"Error loading data to Snowflake")
        raise e

if __name__ == "__main__":
    load_dotenv()

    snowsesh = SnowflakeConnection()
    snowsesh.use_database("INTELLIGENCE_DEV")
    snowsesh.use_schema("AI_CENTRE_DEV")

    try:
        # Retrieve megalith data
        url = 'http://snomed.info/xsct/999000011000230102/version/20230705?fhir_vs=refset'
        refsets = retrieve_megalith(url)
        
        # Load data to Snowflake
        load_megalith_to_snowflake(snowsesh, refsets)
        
    except Exception as e:
        print(f"Error in main process: {e}")
        raise e
        
    snowsesh.session.close()