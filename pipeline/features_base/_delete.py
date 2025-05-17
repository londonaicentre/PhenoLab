from phmlondon.snow_utils import SnowflakeConnection
from phmlondon.feature_store_manager import FeatureStoreManager
from phmlondon.config import SNOWFLAKE_DATABASE, FEATURE_STORE, FEATURE_METADATA
from dotenv import load_dotenv

load_dotenv()

snowsesh = SnowflakeConnection()
snowsesh.use_database(SNOWFLAKE_DATABASE)
snowsesh.use_schema(FEATURE_STORE)

table_to_delete = ""

fsm = FeatureStoreManager(snowsesh, SNOWFLAKE_DATABASE, FEATURE_STORE, FEATURE_METADATA)

snowsesh.use_schema(FEATURE_METADATA)
result = snowsesh.session.sql(f"""
    SELECT feature_id
    FROM feature_registry
    WHERE feature_name = {table_to_delete}
""").collect()

if result:
    feature_id = result[0]["FEATURE_ID"]
    print(f"Feature with ID: {feature_id}")

    fsm.delete_feature(feature_id)
    print(f"Feature {table_to_delete} deleted successfully")
else:
    print(f"Feature {table_to_delete} not found")