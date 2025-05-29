
from dotenv import load_dotenv
from phmlondon.feature_store_manager import FeatureStoreManager

from phmlondon.snow_utils import SnowflakeConnection

load_dotenv()
conn = SnowflakeConnection()
DATABASE = "INTELLIGENCE_DEV"
SCHEMA = "AI_CENTRE_FEATURE_STORE"
METADATASCHEMA = "AI_CENTRE_FEATURE_STORE_METADATA"
feature_store_manager = FeatureStoreManager(conn, DATABASE, SCHEMA, METADATASCHEMA)

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

fid = feature_store_manager.get_feature_id_from_table_name('hypertension_v1')
print(fid)

v = feature_store_manager.get_latest_feature_version(fid)
print(v)

v2 = feature_store_manager.update_feature(
    feature_id=fid,
    new_sql_select_query=query,
    change_description="A test update; same query"
)
print(v2)

