import streamlit as st
# from phmlondon.snow_utils import SnowflakeConnection
from snowflake.snowpark import Session
import os
from dotenv import load_dotenv
from typing import Optional

def get_snowflake_session(connection_parameters: dict) -> Optional[Session]:
    sf_context = os.getenv("SNOWFLAKE_CONTEXT")
    if sf_context is None:
        st.warning("Running locally without snowflake connection.")
        return None
    return Session.builder.configs(connection_parameters).create()

st.title("Snowflake Native App Example")

load_dotenv()
            
connection_parameters = {
        "account": os.getenv("SNOWFLAKE_SERVER"),
        "user": os.getenv("SNOWFLAKE_USER"),
        "role": os.getenv("SNOWFLAKE_USERGROUP"),
        "authenticator": "externalbrowser",
    }

session = get_snowflake_session(connection_parameters)

if session: 
    results = session.sql("SELECT * FROM PROD_DWH.ANALYST_PRIMARY_CARE.CONCEPT LIMIT 10").to_pandas()

    st.write("Results from Snowflake:", results)
else: 
    st.info("No data returned as running in local mode")

