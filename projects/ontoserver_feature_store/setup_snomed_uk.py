import os
import requests
import json
import pandas as pd
from dotenv import load_dotenv
from phmlondon.onto_utils import FHIRTerminologyClient
from phmlondon.snow_utils import confirm_env_vars, create_snowflake_session, select_database_schema
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

def main():
    ## ensure environmental variables are set up
    load_dotenv()

    env_vars = ["CLIENT_ID", "CLIENT_SECRET", 
                "SNOWFLAKE_SERVER", "SNOWFLAKE_USER", "SNOWFLAKE_USERGROUP"]
    confirm_env_vars(env_vars)

    ## sets up snowflake connection and schema location
    session = create_snowflake_session()
    
    select_database_schema(session, "INTELLIGENCE_DEV", "AI_CENTRE_DEV")
    
    ## retrieves SNOMED UK megalith 
    url = 'http://snomed.info/xsct/999000011000230102/version/20230705?fhir_vs=refset'
    refsets = retrieve_megalith(url)
    
    ## creates/confirms table and loads megalith 
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
        session.sql(create_table_query).collect()
        print("Table created successfully.")
    except Exception as e:
        print(f"Error creating table: {e}")
        sys.exit(1)
    
    try:
        session.write_pandas(refsets, 
                             table_name="SNOMED_UK_MEGALITH", 
                             database="INTELLIGENCE_DEV", 
                             schema="AI_CENTRE_DEV",
                             overwrite=True)
        print("Data written to Snowflake successfully.")
    except Exception as e:
        print(f"Error writing data to Snowflake: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
