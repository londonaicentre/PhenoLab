from dotenv import load_dotenv
from phmlondon.feature_store_manager import FeatureStoreManager

from phmlondon.snow_utils import SnowflakeConnection

DATABASE = "INTELLIGENCE_DEV"
SCHEMA = "AI_CENTRE_FEATURE_STORE"
METADATA_SCHEMA = "AI_CENTRE_FEATURE_STORE_METADATA"
load_dotenv()
conn = SnowflakeConnection()
feature_store_manager = FeatureStoreManager(conn, DATABASE, SCHEMA, METADATA_SCHEMA)
feature_store_manager.create_feature_store()