import pandas as pd
from dotenv import load_dotenv

from phmlondon.snow_utils import SnowflakeConnection

# Generates INTELLIGENCE_DEV.AI_CENTRE_FEATURE_STORE.DATA_TYPES

CREATE_DATA_TYPE_TABLE = """
CREATE OR REPLACE TABLE INTELLIGENCE_DEV.AI_CENTRE_FEATURE_STORE.DATA_TYPES AS
SELECT data_type,
COUNT(*) AS COUNT,
--CASE 
--WHEN data_type = NUMBER then float64
--WHEN data_type = VARCHAR then str
--WHEN data_type = TIME then strptime
--WHEN data_type = DATE then date
--WHEN data_type = TEXT then str
--
FROM (
    SELECT data_type FROM intelligence_dev.information_schema.columns
    UNION ALL
    SELECT data_type FROM intelligence_prod.information_schema.columns
    UNION ALL
    SELECT data_type FROM prod_dwh.information_schema.columns
    UNION ALL
    SELECT data_type FROM prod_source.information_schema.columns
) all_types
GROUP BY data_type;
"""

def main():
    load_dotenv()


    try:
        snowsesh = SnowflakeConnection()
        snowsesh.use_database("INTELLIGENCE_DEV")
        snowsesh.use_schema("AI_CENTRE_FEATURE_STORE")

        snowsesh.execute_query(CREATE_DATA_TYPE_TABLE)

        print("Data type table updated")

    except Exception as e:
        print(f"Error creating feature store: {e}")
        raise e
    finally:
        snowsesh.session.close()

if __name__ == "__main__":
    main()
