import streamlit as st
from dotenv import load_dotenv
from utils.database_utils import connect_to_snowflake, get_data_from_snowflake_to_dataframe
from utils.style_utils import set_font_lato

st.set_page_config(page_title="Feature Store Browser", layout="wide", initial_sidebar_state="expanded")

set_font_lato()

load_dotenv()

st.title("AI Centre Definitions")

conn = connect_to_snowflake()

definitions = get_data_from_snowflake_to_dataframe(conn, "SELECT CODELIST_ID, CODELIST_NAME, CODELIST_VERSION, " \
    "DEFINITION_ID, DEFINITION_NAME, DEFINITION_VERSION, VERSION_DATETIME, UPLOADED_DATETIME " \
    "FROM AIC_DEFINITIONS " \
    "GROUP BY CODELIST_ID, CODELIST_NAME, CODELIST_VERSION, " \
    "DEFINITION_ID, DEFINITION_NAME, DEFINITION_VERSION, VERSION_DATETIME, UPLOADED_DATETIME;")
st.dataframe(definitions)