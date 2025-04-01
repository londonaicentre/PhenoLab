from dotenv import load_dotenv
from phmlondon.feature_store_manager import FeatureStoreManager

from phmlondon.snow_utils import SnowflakeConnection

DATABASE = "INTELLIGENCE_DEV"
SCHEMA = "TEST_FEATURE_STORE_IW_2"
load_dotenv()
conn = SnowflakeConnection()
# #we don't pass in a metadata schema, so it will default to using the same schema as for the feature tables
feature_store_manager = FeatureStoreManager(conn, DATABASE, SCHEMA)
# feature_store_manager.create_feature_store()

with open("htn_query.sql", "r") as fid:
    query = fid.read()

feature_store_manager.add_new_feature(
    feature_name="Hypertension",
    feature_desc=""""
        Hypertension either diagnosed by codelist, by ambulatory readings or by 3 clinic readings. Hypertension is 
        categorised as stage 1 if the patient has an ambulatory reading between 135/85 and 149/94 or 3 clinic readings 
        from 140/90 to 179/119 in 1 year, and as stage 2 if the patient has an ambulatory reading above 150/95 or 
        3 clinic readings above 180/120 in 1 year. Patients diagnosed by codelist do not have a stage 1/2 flag.
        """,
    feature_format="Binary/categorical",
    sql_select_query_to_generate_feature=query)