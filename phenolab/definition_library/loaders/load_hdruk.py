import pandas as pd
from snowflake.snowpark import Session

from definition_library.loaders.create_tables import load_definitions_to_snowflake

def retrieve_hdruk_definitions_and_add_to_snowflake(session: Session, database: str = "INTELLIGENCE_DEV",
        schema: str = "AI_CENTRE_DEFINITION_LIBRARY"):
    # The CSV used here is genrated from the HDRUK API using the fetch_hdruk.py script 
    # (decoupled use of the API from the loader to allow for running on streamlit in snowflake without complicated
    # workarounds to install non-anaconda packages)
    df = pd.read_csv("definition_library/loaders/data/hdruk/hdruk_definitions.csv") 
    print(f"Loaded HDRUK definitions from CSV file - {len(df)} rows")
    load_definitions_to_snowflake(session=session, df=df, table_name="HDRUK_DEFINITIONS", 
        database=database, schema=schema)

if __name__ == "__main__":
    session = Session.builder.config("connection_name", "nel_icb").create()
    retrieve_hdruk_definitions_and_add_to_snowflake(session=session, database="INTELLIGENCE_DEV", 
        schema="AI_CENTRE_DEFINITION_LIBRARY")
