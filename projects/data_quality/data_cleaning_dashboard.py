"""
Script to run a streamlit app which visualisation of some data quality bits
"""

from datetime import datetime

import helper_functions as hf
import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from dotenv import load_dotenv
from plotly.subplots import make_subplots

from phmlondon.snow_utils import SnowflakeConnection

@st.cache_resource
def make_dq_cached(db, schema):
    return hf.DataQuality(db, schema)

app_explanation = """
## What is this for?
This is a dashboard to visualise the different distributions and data types
of the data that are available, designed mostly around the observations table

## How to use this applet
1. Choose a database, schema and table that you are interested in
2. Choose a column
3. The app will show you some relevant data about that column
4. You can subselect some data about this column too
"""

def main() -> None: #noqa: C901
    ## Set up the session ##
    load_dotenv()

    if "snowsesh" not in st.session_state:
        db = 'INTELLIGENCE_DEV'
        schema = "AI_CENTRE_PHENOTYPE_LIBRARY"
        #st.session_state.snowsesh = SnowflakeConnection()
        #st.session_state.snowsesh.use_database(db)
        #st.session_state.snowsesh.use_schema(schema)
        data_qual = make_dq_cached(db, schema)

    #snowsesh = st.session_state.snowsesh
    st.title("Data Cleaning Dashboard")
    st.write(
        """
        Dashboard to work through the cleaning steps for different columns in the observations table
        """
    )

    st.markdown(app_explanation)

    #Put the possible table/ schema choices
    db_schCol, tab_cCol = st.columns(2)
    with db_schCol:
        #Choose DB
        db_select = st.selectbox('Select Database', data_qual.show_databases())
        data_qual.current_database = db_select

        #Choose Schema
        sch_select = st.selectbox('Select Schema', data_qual.show_schemas())
        data_qual.current_schema = sch_select


    with tab_cCol:
        #Choose table
        tab_select = st.selectbox('Select Table', data_qual.show_tables())
        data_qual.current_table = tab_select

        #Choose column
        col_select = st.selectbox('Select Column', data_qual.show_columns())
        data_qual.current_column = col_select

if __name__ == "__main__":
    main()
