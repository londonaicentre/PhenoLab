import streamlit as st
from dotenv import load_dotenv
from utils.database_utils import connect_to_snowflake, get_data_from_snowflake_to_dataframe
from utils.style_utils import set_font_lato

st.set_page_config(page_title="Feature Store Browser", layout="wide", initial_sidebar_state="expanded")

set_font_lato()

load_dotenv()

st.title("Feature Store Browser")

conn = connect_to_snowflake()

@st.cache_data
def switch_schema():
    conn.use_schema("AI_CENTRE_FEATURE_STORE_METADATA")

switch_schema()

st.header('Features')
features = get_data_from_snowflake_to_dataframe(conn, "SELECT * FROM FEATURE_REGISTRY")

st.dataframe(features)

st.header('Feature versions')
versions = get_data_from_snowflake_to_dataframe(conn, "SELECT * FROM FEATURE_VERSION_REGISTRY")

st.dataframe(versions)
