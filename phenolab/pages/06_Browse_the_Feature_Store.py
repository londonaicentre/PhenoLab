import streamlit as st

from utils.database_utils import get_data_from_snowflake_to_dataframe, get_snowflake_session
from utils.style_utils import set_font_lato
from utils.config_utils import load_config

# # 07_Browse_the_Feature_Store.py

# Allows users to browse the feature store contents, including available /
# features and their metadata.


st.set_page_config(page_title="Feature Store Browser", layout="wide", initial_sidebar_state="expanded")

set_font_lato()
if "session" not in st.session_state:
    st.session_state.session = get_snowflake_session()
if "config" not in st.session_state:
    st.session_state.config = load_config()
    
# load_dotenv()

st.title("Feature Store Browser")

if st.button("Refresh Data"):
    get_data_from_snowflake_to_dataframe.clear()
    st.rerun()

st.header('Features')

features_query = f"""
    SELECT * FROM 
    {st.session_state.config['feature_store']['database']}.
    {st.session_state.config['feature_store']['metadata_schema']}.FEATURE_REGISTRY
"""
features = st.session_state.session.sql(features_query).to_pandas()
st.dataframe(features)


st.header('Feature versions')
versions_query = f"""
    SELECT * FROM 
    {st.session_state.config['feature_store']['database']}.
    {st.session_state.config['feature_store']['metadata_schema']}.FEATURE_VERSION_REGISTRY
"""
versions = st.session_state.session.sql(versions_query).to_pandas()
st.dataframe(versions)
