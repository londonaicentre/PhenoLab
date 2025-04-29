"""
This script uses the existing SQL queries to recreate the master person index and IMD2019LONDON tables that JZ was using
for dashboarding as "features" using the Feature Store Manager.
IW, 29-04-2025
"""

from dotenv import load_dotenv
from phmlondon.feature_store_manager import FeatureStoreManager
from phmlondon.snow_utils import SnowflakeConnection
import pandas as pd
from person_master_index import CREATE_MASTER_INDEX_SQL, create_ethnicity_mapping_case_statement
import os

load_dotenv()
# print("Environment variables loaded")
# print(os.environ['SNOWFLAKE_SERVER'])
conn = SnowflakeConnection()
print("Snowflake connection created")
DATABASE = "INTELLIGENCE_DEV"
SCHEMA = "AI_CENTRE_FEATURE_STORE"
METADATASCHEMA = "AI_CENTRE_FEATURE_STORE_METADATA"
feature_store_manager = FeatureStoreManager(conn, DATABASE, SCHEMA, METADATASCHEMA)
print("Feature store manager created")

# 1. Create a temp table for IMD
df = pd.read_csv("data/imd2019london.csv")
df.columns = [col.upper() for col in df.columns]
conn.use_schema('AI_CENTRE_DEV')
conn.load_dataframe_to_table(df=df, table_name="IMD2019LONDON", mode="overwrite")
print("IMD2019LONDON loaded successfully")

# 2. Create IMD as a feature
feature_store_manager.add_new_feature(
    feature_name="IMD2019LONDON",
    feature_desc="""
        IMD2019LONDON data loaded from CSV file. This table contains the IMD2019 data for London boroughs.
        The IMD2019 data is used to calculate the IMD score for each patient in the master index.
    """,
    feature_format="Categorical",
    sql_select_query_to_generate_feature="""
        SELECT * FROM INTELLIGENCE_DEV.AI_CENTRE_DEV.IMD2019LONDON
    """,
    existence_ok=True,
)

#  3. Delete temporary table
conn.session.sql(f"""DROP TABLE IF EXISTS INTELLIGENCE_DEV.AI_CENTRE_DEV.IMD2019LONDON""").collect()

# 4. Create master person index as a feature
ethnicity_case = create_ethnicity_mapping_case_statement()
master_person_sql = CREATE_MASTER_INDEX_SQL.format(ethnicity_case=ethnicity_case)
prefix = """
CREATE OR REPLACE TABLE INTELLIGENCE_DEV.AI_CENTRE_FEATURE_STORE.PERSON_NEL_MASTER_INDEX AS"""
master_person_sql = master_person_sql.removeprefix(prefix)
master_person_sql = master_person_sql.replace("INTELLIGENCE_DEV.AI_CENTRE_FEATURE_STORE.IMD2019LONDON", "INTELLIGENCE_DEV.AI_CENTRE_FEATURE_STORE.IMD2019LONDON_V1")

feature_store_manager.add_new_feature(
    feature_name="Person Master Index",
    feature_desc="""
        Uses the NEL person master index to create an index of patients who are currently registered and resident, DOB,
        DOD, and IMD data. The IMD data is loaded from the IMD2019LONDON table.
    """,
    feature_format="Categorical",
    sql_select_query_to_generate_feature=master_person_sql,
    existence_ok=True,
)