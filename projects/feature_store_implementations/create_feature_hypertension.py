
from dotenv import load_dotenv
from phmlondon.snow_utils import SnowflakeConnection
from feature_store_manager import FeatureStoreManager

load_dotenv()
conn = SnowflakeConnection()
DATABASE = "INTELLIGENCE_DEV"
SCHEMA = "TEST_FEATURE_STORE_IW_2"
feature_store_manager = FeatureStoreManager(conn, DATABASE, SCHEMA)

with open("htn_query.sql", "r") as fid:
    query = fid.read()

# df = conn.session.sql(query).to_pandas()
# # print(df.head(100))

feature_store_manager.add_new_feature(
    feature_name="Hypertension",
    feature_desc="Hypertension either diagnosed by codelist, by ambulatory readings or by 3 clinic readings",
    feature_format="Binary/categorical",
    sql_select_query_to_generate_feature=query,
    target_lag="7 days")