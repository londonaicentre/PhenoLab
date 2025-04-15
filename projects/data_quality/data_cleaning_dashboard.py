"""
Script to run a streamlit app which visualisation of some data quality bits
"""

import re
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


    #Some metrics about the current column
    with st.form("choose_cols"):
        st.write('Get some stats')
        submitted_cols = st.form_submit_button()
        if submitted_cols:
            #Some metrics about the current column
            col1, col2, col3 = st.columns(3)

            #pull out dtype and clean off the extras
            col_dtype = data_qual.dtype(
                            data_qual.current_table,
                            data_qual.current_column
                            )
            col_dtype = re.sub('^([a-z]+).*', '\\1', col_dtype, flags=re.IGNORECASE)

            #Get table length and work out how many null values
            n_values = data_qual.table_length(data_qual.current_table)
            prop_null = data_qual.proportion_null(
                            data_qual.current_table,
                            data_qual.current_column
                            )

            #Get the mean
            col_mean = data_qual.mean(data_qual.current_table,
                                      data_qual.current_column)

            with col1:
                st.metric("Data Type",col_dtype)
                if col_dtype in ['NUMBER',
                                 'DECIMAL',
                                 'NUMERIC',
                                 'INT',
                                 'INTEGER',
                                 'BIGINT',
                                 'SMALLINT',
                                 'TINYINT',
                                 'BYTEINT',
                                 'FLOAT',
                                 'DOUBLE',
                                 'REAL']:
                    st.metric('Column Mean', col_mean)
            with col2:
                st.metric("Number of values", n_values)
            with col3:
                st.metric("Proportion Null",
                        f"""{round(prop_null*100, ndigits = 2)}%"""
                        )

            #with st.form('subset_cols'):
            #    st.


if __name__ == "__main__":
    main()
