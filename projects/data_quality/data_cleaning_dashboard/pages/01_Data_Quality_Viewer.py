"""
Script to run a streamlit app which visualisation of some data quality bits
"""

import re

import pandas as pd
import plotly.express as px
import streamlit as st
import utils.helper_functions as hf
from dotenv import load_dotenv

from phmlondon.snow_utils import SnowflakeConnection


@st.cache_resource
def make_dq_cached(_snowsesh, db, schema):
    return hf.DataQuality(_snowsesh, db, schema)

@st.cache_resource
def pull_df(query: str, _data_qual_obj: hf.DataQuality) -> pd.DataFrame:
    return _data_qual_obj.execute_query_to_table(query)

app_explanation = """
## What is this for?
This is a dashboard to visualise the different distributions and data types
of the data that are available, designed mostly around the observations table

## How to use this applet
1. Choose a database, schema and table that you are interested in
2. Choose a column
3. The app will show you some relevant data about that column
4. You can subselect some data about this column too

N.B: Wait for each box to update when changing the previous one, otherwise it throws an error
"""
numeric_cols = ['NUMBER',
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
                'REAL']


def main() -> None:
    ## Set up the session ##
    load_dotenv()

    if "snowsesh" not in st.session_state:
        db = 'INTELLIGENCE_DEV'
        schema = "AI_CENTRE_PHENOTYPE_LIBRARY"
        st.session_state.snowsesh = SnowflakeConnection()
        #st.session_state.snowsesh.use_database(db)
        #st.session_state.snowsesh.use_schema(schema)
        data_qual = make_dq_cached(st.session_state.snowsesh, db, schema)

    try:
        isinstance(data_qual, hf.DataQuality)
    except:
        db = 'INTELLIGENCE_DEV'
        schema = "AI_CENTRE_PHENOTYPE_LIBRARY"
        data_qual = make_dq_cached(st.session_state.snowsesh, db, schema)

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
            col1, col2, col3, col4 = st.columns(4)

            #pull out dtype and clean off the extras
            col_dtype = data_qual.dtype(
                            data_qual.current_table,
                            data_qual.current_column
                            )
            col_dtype = re.sub('^([a-z]+).*', '\\1', col_dtype, flags=re.IGNORECASE)

            #Make this an attribute of data_qal
            data_qual.col_dtype = col_dtype
            data_qual.col_numeric = col_dtype in numeric_cols

            #Get table length and work out how many null values
            n_values = data_qual.table_length(data_qual.current_table)
            prop_null = data_qual.proportion_null(
                            data_qual.current_table,
                            data_qual.current_column
                            )

            #Dtype
            with col1:
                st.metric("Data Type", data_qual.col_dtype)

            #N values
            with col2:
                st.metric("Number of values", n_values)

            #Proportion null
            with col3:
                st.metric("Proportion Null",
                        f"""{round(prop_null*100, ndigits = 2)}%"""
                        )

            #Mean
            with col4:
                if data_qual.col_numeric:
                    col_mean = data_qual.mean(data_qual.current_table,
                                        data_qual.current_column)
                    st.metric('Column Mean', col_mean)

            #Plot the data
            col_query = f"""SELECT {data_qual.current_column}
                        FROM {data_qual.long_table(data_qual.current_table)}
                        WHERE {data_qual.current_column} IS NOT NULL
                        Limit 1000000"""
            col_df = pull_df(col_query, data_qual)
            fig_distr = px.histogram(
                col_df,
                x=str(data_qual.current_column),#,
                #nbins=died_bins,  # Slider from above,
                )
            st.plotly_chart(fig_distr)

    #Now pick units for subsetting
    col1, col2 = st.columns(2)
    with col1:
        second_col = st.selectbox('Select Second Column', data_qual.show_columns())

    with col2:
        subset_query = f"""SELECT DISTINCT {second_col}
                        FROM {data_qual.long_table(data_qual.current_table)} LIMIT 100"""
        subsets = pull_df(subset_query, data_qual)
        choose_subset = st.selectbox('Choose Second Criteria', subsets.iloc[:, 0].to_list())

    with st.form("choose_second_col"):
        st.write('Plot Subdistributions')
        submitted_subsets = st.form_submit_button()
        if submitted_subsets:
            subset_distr_query = f""" SELECT {data_qual.current_column} FROM
                                    {data_qual.long_table(data_qual.current_table)}
                                    WHERE {second_col} = '{choose_subset}'
                                    AND {data_qual.current_column} IS NOT NULL
                                    LIMIT 500000"""
            subset_df = data_qual.execute_query_to_table(subset_distr_query)
            fig_subset = px.histogram(
                subset_df,
                x=str(data_qual.current_column),#,
                #nbins=died_bins,  # Slider from above,
                )
            st.plotly_chart(fig_subset)

if __name__ == "__main__":
    main()
