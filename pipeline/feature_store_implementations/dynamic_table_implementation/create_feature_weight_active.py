
from dotenv import load_dotenv
from feature_store_manager import FeatureStoreManager

from phmlondon.snow_utils import SnowflakeConnection

load_dotenv()
conn = SnowflakeConnection()
DATABASE = "INTELLIGENCE_DEV"
SCHEMA = "AI_CENTRE_FEATURE_STORE"
feature_store_manager = FeatureStoreManager(conn, DATABASE, SCHEMA)

with open("weight_table_active.sql", "r") as fid:
    query = fid.read()

feature_store_manager.add_new_feature(
    feature_name="Weight_active",
    feature_desc=""""
        Weight categorisation determined by either 1.BMI, 2.Coded category, 3.BMI calculations
        from Height and Weight.
        Categories: Underweight, Healthy weight, Overweight,
        Obese unclassified, Obese class I, Obese class II, Obese class III
        Cohort - active patients only and most recent result.
        Active status defined by NEL master person index table
        """,
    feature_format="Binary/categorical",
    sql_select_query_to_generate_feature=query)

# fid = feature_store_manager.get_feature_id_from_table_name('hypertension_v1')
# print(fid)

# v = feature_store_manager.get_latest_feature_version(fid)
# print(v)

# v2 = feature_store_manager.update_feature(
#     feature_id=fid,
#     new_sql_select_query=query,
#     change_description="A test update; same query"
# )
# print(v2)

