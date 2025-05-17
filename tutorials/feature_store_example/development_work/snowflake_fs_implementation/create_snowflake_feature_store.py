from snowflake.ml.feature_store import FeatureStore, CreationMode
from dotenv import load_dotenv
from phmlondon.snow_utils import SnowflakeConnection
import logging

logging.basicConfig(level=logging.DEBUG)

load_dotenv()
conn = SnowflakeConnection()

fs = FeatureStore(
        session=conn.session,
        database="INTELLIGENCE_DEV",
        name="TEST_FEATURE_STORE_IW",
        default_warehouse="INTELLIGENCE_XS",
        creation_mode=CreationMode.CREATE_IF_NOT_EXIST,
     )

