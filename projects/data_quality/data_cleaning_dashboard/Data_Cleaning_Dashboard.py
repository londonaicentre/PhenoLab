import streamlit as st

st.set_page_config(page_title="Data Cleaning Dashboard", layout="wide", initial_sidebar_state="expanded")

st.title("Data Cleaning Dashboard")

st.markdown("""
This dashboard is for:
1. **Data Quality**: Viewing data quality for different tables
2. **Data Cleaning**: Working through data cleaning steps for diffent definitions (these need to be made in PhenoLab)
""")

with st.expander("Environment Variables Required"):
    st.code("""
    Required environment variables as `.env`:
    - SNOWFLAKE_SERVER: Snowflake account identifier
    - SNOWFLAKE_USER: Snowflake username
    - SNOWFLAKE_USERGROUP: Snowflake role
    """)

st.markdown("---")
st.markdown("2025 London AI Centre & OneLondon")
