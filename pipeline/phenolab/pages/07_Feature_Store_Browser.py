import streamlit as st
from dotenv import load_dotenv
from utils.database_utils import connect_to_snowflake, get_data_from_snowflake_to_dataframe
from utils.style_utils import set_font_lato
from phmlondon.snow_utils import SnowflakeConnection

st.set_page_config(page_title="Feature Store Browser", layout="wide", initial_sidebar_state="expanded")

set_font_lato()

load_dotenv()

st.title("Feature Store Browser")

conn = connect_to_snowflake()

@st.cache_data
def switch_schema(_conn: SnowflakeConnection, schema: str = "AI_CENTRE_FEATURE_STORE_METADATA"):
    _conn.use_schema(schema)

switch_schema(conn)

st.header('Features')
features_query = "SELECT * FROM FEATURE_REGISTRY"
features = get_data_from_snowflake_to_dataframe(conn, features_query)

st.dataframe(features)

st.header('Feature versions')
versions_query = "SELECT * FROM FEATURE_VERSION_REGISTRY"
versions = get_data_from_snowflake_to_dataframe(conn, versions_query)

st.dataframe(versions)

if st.button("Refresh"):
    get_data_from_snowflake_to_dataframe.clear(conn, features_query)
    get_data_from_snowflake_to_dataframe.clear(conn, versions_query)

switch_schema(conn, "AI_CENTRE_DEFINITION_LIBRARY")