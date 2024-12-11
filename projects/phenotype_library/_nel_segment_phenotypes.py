import os
from dotenv import load_dotenv
from phmlondon.snow_utils import SnowflakeConnection

def create_phenotype_table(snowsesh):
    """
    Creates NEL_SEGMENT_PHENOTYPES table from segmentation codes
    """
    query = """
    CREATE OR REPLACE TABLE NEL_SEGMENT_PHENOTYPES AS
    SELECT DISTINCT 
        CONDITION_UPPER,
        DBID,
        CODE,
        SCHEME,
        DESCRIPTION
    FROM INTELLIGENCE_DEV.PHM_SEGMENTATION.SEGMENTATION_CODES
    WHERE SCHEME IN ('SNOMED', 'ICD10')
    ORDER BY CONDITION_UPPER
    """
    
    try:
        snowsesh.execute_query(query)
        print("Table created successfully")
        
    except Exception as e:
        print(f"Error creating NEL segment phenotype table: {e}")
        raise e

if __name__ == "__main__":
    load_dotenv()
    
    try:
        snowsesh = SnowflakeConnection()
        
        snowsesh.use_database("INTELLIGENCE_DEV")
        snowsesh.use_schema("AI_CENTRE_PHENOTYPE_LIBRARY")
        
        create_phenotype_table(snowsesh)
        
    except Exception as e:
        print(f"Error creating phenotype table: {e}")
        raise e
    finally:
        snowsesh.session.close()