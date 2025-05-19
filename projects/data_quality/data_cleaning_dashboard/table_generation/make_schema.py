from dotenv import load_dotenv

from phmlondon.snow_utils import SnowflakeConnection

# Generates INTELLIGENCE_DEV.AI_CENTRE_OBSERVATION_STAGING_TABLES

CREATE_DATA_TYPE_TABLE = """
CREATE OR REPLACE SCHEMA INTELLIGENCE_DEV.AI_CENTRE_OBSERVATION_STAGING_TABLES
COMMENT = 'Schema for storing staging tables for cleaned observations';
"""

def main():
    load_dotenv()


    try:
        snowsesh = SnowflakeConnection()
        snowsesh.use_database("INTELLIGENCE_DEV")
        snowsesh.use_schema("AI_CENTRE_FEATURE_STORE")

        snowsesh.execute_query(CREATE_DATA_TYPE_TABLE)

        print("Cleaned observations schema updated")

    except Exception as e:
        print(f"Error creating schema: {e}")
        raise e
    finally:
        snowsesh.session.close()

if __name__ == "__main__":
    main()