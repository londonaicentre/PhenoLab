import streamlit as st
from dotenv import load_dotenv
from phmlondon.snow_utils import SnowflakeConnection

# This code when run with streamlit run will open a dashboard to investigate the assocaition of calculated pdc vs recorded compliance statement (good or poor)
# Still massively a work in progress


load_dotenv()


if 'snowsesh' not in st.session_state:
        st.session_state.snowsesh = SnowflakeConnection()
        st.session_state.snowsesh.use_database("INTELLIGENCE_DEV")
        st.session_state.snowsesh.use_schema("AI_CENTRE_FEATURE_STORE")

snowsesh = st.session_state.snowsesh

query = """ 
    SELECT 
        overall_compliance_score,
        medication_compliance
    FROM 
        intelligence_dev.ai_centre_dev.comp_person_pdc
    LIMIT 1000
"""

# Execute query and fetch data into a pandas DataFrame
df = snowsesh.query(query)  # This should not be indented inside an unexpected block
st.dataframe(df)