import pandas as pd
import streamlit as st
from snowflake.snowpark import Session

from definition_library.loaders.create_tables import load_definitions_to_snowflake

def retrieve_open_codelists_definitions_and_add_to_snowflake(
        database: str = "INTELLIGENCE_DEV",
        schema: str = "AI_CENTRE_DEFINITION_LIBRARY"):
    df = pd.read_csv("definition_library/loaders/data/open_codelists_compiled/open_codelists_definitions.csv")
    print(f"Loaded OpenCodelists definitions from CSV file - {len(df)} rows")
    load_definitions_to_snowflake(session=st.session_state.session, df=df, table_name="OPEN_CODELISTS", 
        database=database, schema=schema)
    
if __name__ == "__main__":
    session = Session.builder.config("connection_name", "nel_icb").create()
    retrieve_open_codelists_definitions_and_add_to_snowflake(session=session,
        database="INTELLIGENCE_DEV", schema="AI_CENTRE_DEFINITION_LIBRARY")
