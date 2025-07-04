import pandas as pd
import streamlit as st
from snowflake.snowpark import Session

from definition_library.loaders.create_tables import load_definitions_to_snowflake

def retrieve_hdruk_definitions_and_add_to_snowflake(
        database: str = "INTELLIGENCE_DEV",
        schema: str = "AI_CENTRE_DEFINITION_LIBRARY"):
    # The parquet file used here is generated from the HDRUK API using the fetch_hdruk.py script 
    # (decoupled use of the API from the loader to allow for running on streamlit in snowflake without complicated
    # workarounds to install non-anaconda packages)
    df = pd.read_parquet("definition_library/loaders/data/hdruk/hdruk_definitions.parquet") 
    print(f"Loaded HDRUK definitions from parquet file - {len(df)} rows")
    load_definitions_to_snowflake(session=st.session_state.session, df=df, table_name="HDRUK_DEFINITIONS", 
        database=database, schema=schema)

if __name__ == "__main__":
    session = Session.builder.config("connection_name", "nel_icb").create()
    retrieve_hdruk_definitions_and_add_to_snowflake(session=session, database="INTELLIGENCE_DEV", 
        schema="AI_CENTRE_DEFINITION_LIBRARY")
