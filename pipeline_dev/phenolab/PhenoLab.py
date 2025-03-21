import streamlit as st

st.set_page_config(page_title="PhenoLab", layout="wide", initial_sidebar_state="expanded")

st.title("PhenoLab: Clinical Definitions and Phenotype Creator")

st.markdown("""
PhenoLab helps manage:
1. **Medical Definitions**: Collections of clinical codes from different vocabularies (SNOMED, ICD10, etc.) that represent specific medical concepts
2. **Phenotypes**: Patient-centric clinical labels based on logical operations applied to these definitions
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
