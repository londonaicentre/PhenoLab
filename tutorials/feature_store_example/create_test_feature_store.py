from dotenv import load_dotenv
from phmlondon.feature_store_manager import FeatureStoreManager

from phmlondon.snow_utils import SnowflakeConnection

DATABASE = "INTELLIGENCE_DEV"
SCHEMA = "TEST_FEATURE_STORE_IW_2"
load_dotenv()
conn = SnowflakeConnection()
# #we don't pass in a metadata schema, so it will default to using the same schema as for the feature tables
feature_store_manager = FeatureStoreManager(conn, DATABASE, SCHEMA)
feature_store_manager.create_feature_store()