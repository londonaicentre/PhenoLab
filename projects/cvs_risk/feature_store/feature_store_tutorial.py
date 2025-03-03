from snowflake.ml.feature_store import FeatureStore, CreationMode
from snowflake.ml.feature_store.examples.example_helper import ExampleHelper
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

for e in example_helper.load_entities():
    fs.register_entity(e)
all_entities_df = fs.list_entities()
all_entities_df.show()