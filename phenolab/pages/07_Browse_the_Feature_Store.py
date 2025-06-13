import streamlit as st
from dotenv import load_dotenv
from utils.database_utils import get_data_from_snowflake_to_dataframe, get_snowflake_session
from utils.style_utils import set_font_lato

from phmlondon.config import FEATURE_METADATA, SNOWFLAKE_DATABASE

# # 07_Browse_the_Feature_Store.py

# Allows users to browse the feature store contents, including available /
# features and their metadata.


st.set_page_config(page_title="Feature Store Browser", layout="wide", initial_sidebar_state="expanded")

set_font_lato()

load_dotenv()

st.title("Feature Store Browser")

if st.button("Refresh Data"):
    get_data_from_snowflake_to_dataframe.clear()
    st.rerun()

# conn = get_snowflake_connection()
session = get_snowflake_session()

st.header('Features')

# session.use_database(SNOWFLAKE_DATABASE)
# session.use_schema(FEATURE_METADATA)
features_query = f"SELECT * FROM {SNOWFLAKE_DATABASE}.{FEATURE_METADATA}.FEATURE_REGISTRY"
features = session.sql(features_query).to_pandas()
st.dataframe(features)
# with conn.use_context(database=SNOWFLAKE_DATABASE, schema=FEATURE_METADATA):
#     features_query = "SELECT * FROM FEATURE_REGISTRY"
#     features = get_data_from_snowflake_to_dataframe(conn, features_query)
#     st.dataframe(features)

st.header('Feature versions')
# session.use_database(SNOWFLAKE_DATABASE)
# session.use_schema(FEATURE_METADATA)
versions_query = f"SELECT * FROM {SNOWFLAKE_DATABASE}.{FEATURE_METADATA}.FEATURE_VERSION_REGISTRY"
versions = session.sql(versions_query).to_pandas()
st.dataframe(versions)
# with conn.use_context(database=SNOWFLAKE_DATABASE, schema=FEATURE_METADATA):
#     versions_query = "SELECT * FROM FEATURE_VERSION_REGISTRY"
#     versions = get_data_from_snowflake_to_dataframe(conn, versions_query)
#     st.dataframe(versions)
