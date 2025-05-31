import streamlit as st
from dotenv import load_dotenv
from utils.database_utils import get_snowflake_connection, get_data_from_snowflake_to_dataframe
from utils.style_utils import set_font_lato

from phmlondon.config import DEFINITION_LIBRARY, FEATURE_METADATA, SNOWFLAKE_DATABASE

# # 07_Browse_the_Feature_Store.py

# Allows users to browse the feature store contents, including available /
# features and their metadata.


st.set_page_config(page_title="Feature Store Browser", layout="wide", initial_sidebar_state="expanded")

set_font_lato()

load_dotenv()

st.title("Feature Store Browser")

conn = get_snowflake_connection()

st.header('Features')
# Use context manager for schema switching
with conn.use_context(database=SNOWFLAKE_DATABASE, schema=FEATURE_METADATA):
    features_query = "SELECT * FROM FEATURE_REGISTRY"
    features = get_data_from_snowflake_to_dataframe(conn, features_query)
    st.dataframe(features)

st.header('Feature versions')
with conn.use_context(database=SNOWFLAKE_DATABASE, schema=FEATURE_METADATA):
    versions_query = "SELECT * FROM FEATURE_VERSION_REGISTRY"
    versions = get_data_from_snowflake_to_dataframe(conn, versions_query)
    st.dataframe(versions)

if st.button("Refresh"):
    get_data_from_snowflake_to_dataframe.clear(conn, features_query)
    get_data_from_snowflake_to_dataframe.clear(conn, versions_query)
    st.rerun()