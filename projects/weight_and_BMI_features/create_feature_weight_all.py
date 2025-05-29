from dotenv import load_dotenv
from phmlondon.feature_store_manager import FeatureStoreManager

from phmlondon.snow_utils import SnowflakeConnection

load_dotenv()
conn = SnowflakeConnection()
DATABASE = "INTELLIGENCE_DEV"
SCHEMA = "AI_CENTRE_FEATURE_STORE"
METADATASCHEMA = "AI_CENTRE_FEATURE_STORE_METADATA"
feature_store_manager = FeatureStoreManager(conn, DATABASE, SCHEMA, METADATASCHEMA)

with open("weight_table_all.sql", "r") as fid:
    query = fid.read()

feature_store_manager.add_new_feature(
    feature_name="BMI_classification_all",
    feature_desc="""
        BMI categorisation determined by either 1.BMI, 2.Coded category, 3.BMI calculations
        from Height and Weight.
        Categories: Underweight, Healthy weight, Overweight,
        Obese unclassified, Obese class I, Obese class II, Obese class III
        Cohort - all patients and all results.
        """,
    feature_format="Binary/categorical",
    sql_select_query_to_generate_feature=query)



