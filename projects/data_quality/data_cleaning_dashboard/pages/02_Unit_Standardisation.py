"""
Script to run a streamlit app which visualisation of some data quality bits
"""
import os
import re

import pandas as pd
import plotly.express as px
import streamlit as st
import utils.helper_functions as hf
from utils.database_utils import connect_to_snowflake
from utils.definition_page_display_utils import (
    display_code_search_panel,
    display_selected_codes,
    find_codes_from_existing_phenotypes,
    load_definition,
    load_definitions_list,
)

from phmlondon.snow_utils import SnowflakeConnection


@st.cache_resource
def pull_df(query: str, _data_qual_obj: hf.DataQuality) -> pd.DataFrame:
    return _data_qual_obj.execute_query_to_table(query)

@st.cache_data
def get_definition_sources(_connection: SnowflakeConnection) -> list:
    def_source_query =  """SELECT DISTINCT DEFINITION_SOURCE
                        FROM intelligence_dev.ai_centre_definition_library.definitionstore
                        """
    def_source_df = _connection.execute_query_to_df(def_source_query)
    return def_source_df.iloc[:, 0].to_list()

@st.cache_data
def get_definitions(source: str, _connection: SnowflakeConnection) -> list:
    def_query = f"""SELECT DISTINCT DEFINITION_NAME
                FROM intelligence_dev.ai_centre_definition_library.definitionstore 
                WHERE code is not null
                and DEFINITION_SOURCE = '{source}'
                """
    def_df = _connection.execute_query_to_df(def_query)
    return def_df.iloc[:, 0].to_list()

@st.cache_data
def get_unit_info(definition: str, _connection: SnowflakeConnection) -> pd.DataFrame:
    info_query = f"""SELECT DISTINCT RESULT_VALUE_UNITS,
    COUNT(*) count_units,
    AVG(result_value) mean,
    median(result_value) med,
    min(result_value) minimum_value,
    max(result_value) maximum_value
    FROM PROD_DWH.ANALYST_PRIMARY_CARE.OBSERVATION obs
    left join intelligence_dev.ai_centre_definition_library.definitionstore def on obs.core_concept_id = def.dbid
    where result_value is not null
    and result_value_units is not null
    and def.DEFINITION_NAME ='{definition}'
    GROUP BY RESULT_VALUE_UNITS
    ORDER BY count_units desc"""
    return _connection.execute_query_to_df(info_query)

def get_unit_distributions(definition: str,
                            units: list[str],
                            _connection: SnowflakeConnection) -> pd.DataFrame:
    unit_distr_query = f"""SELECT RESULT_VALUE,
    RESULT_VALUE_UNITS
    FROM PROD_DWH.ANALYST_PRIMARY_CARE.OBSERVATION obs
    left join intelligence_dev.ai_centre_definition_library.definitionstore def on obs.core_concept_id = def.dbid
    where result_value is not null
    and result_value_units is not null
    and def.DEFINITION_NAME ='{definition}'
    and obs.result_value_units in ({', '.join('\'' + unit + '\'' for unit in units)})
    LIMIT 1000000"""
    return _connection.execute_query_to_df(unit_distr_query)

def display_unit_panel(_connection: SnowflakeConnection) -> str:
    """
    Display top panel
    Components for existing unit selection, and new unit creation
    Returns:
        str:
            New unit name
    """
    st.subheader("Enter standardised unit:")
    col1, col2, col3 = st.columns([2, 1, 1])

    # new unit name input
    with col1:
        new_unit_name = st.text_input("New standardised unit name", label_visibility="collapsed")

    # load in definitions
    with col2:
        selected_definition_source = st.selectbox(
            "Definition Source",
            options=get_definition_sources(_connection),
            label_visibility="collapsed"
        )
        definitions_list = get_definitions(selected_definition_source, _connection)
        selected_definition = st.selectbox(
            "Custom definition list",
            options=definitions_list,
            label_visibility="collapsed",
            index=0 if definitions_list else None,
        )

    # component: new definition button
    with col3:
        if st.button("Create unit") and new_unit_name:
            st.session_state.current_unit = new_unit_name
            print(st.session_state.current_unit)
            if "used_checkbox_keys" in st.session_state:
                for checkbox_key in st.session_state.used_checkbox_keys:
                    st.session_state[checkbox_key] = False
            with col1:
                st.success(f"Created new unit: {new_unit_name}")

    unit_df = get_unit_info(selected_definition, _connection)
    unit_df.columns = ['Units',
                       'Count',
                       'Mean',
                       'Median',
                       'Minimum',
                       'Maximum']
    unit_df["plotter"] = False

    edited_unit_df = st.data_editor(
        unit_df,
        width = 1000,
        column_config={
            "plotter": st.column_config.CheckboxColumn(
                "Plot Distribution",
                help="Select units to plot distribution",
                default=False,
                )
                },
        disabled=["widgets"],
        hide_index=True,
    )

    with st.form('unit_distributions'):
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            xmin = st.number_input('XMin')

        with col2:
            xmax = st.number_input('XMax')

        with col3:
            nbinsx = st.number_input('Number of Bins')

        with col4:
            unit_submit = st.form_submit_button()

        if unit_submit:
            checked_units = edited_unit_df.Units[edited_unit_df.plotter].to_list()

            #Now pull out the values for the checked boxes
            unit_distr_df = get_unit_distributions(selected_definition, checked_units, _connection)

            unit_distr_plot = px.histogram(
                unit_distr_df,
                x='RESULT_VALUE',
                color = 'RESULT_VALUE_UNITS',
                range_x=[xmin, xmax],
                nbins = round(nbinsx),
                marginal= 'violin'
                #nbins=died_bins,  # Slider from above,
                )
            st.plotly_chart(unit_distr_plot)

    st.markdown("---")

    return new_unit_name


def main():
    st.set_page_config(page_title="Create a new standardised unit", layout="wide")
    st.title("Create a new unit")

    snowsesh = connect_to_snowflake()

    # state variables
    ## the definition that is loaded (or created) and currently being worked on
    if "current_unit" not in st.session_state:
        st.session_state.current_unit = None

    ### the vocab    
    #if "codes" not in st.session_state:
    #    st.session_state.codes = None

    # 1. check if codes are loaded
    #if st.session_state.codes is None:
    #    st.warning("Please load a vocabulary from the Load Vocabulary page first.")
    #    return

    # 2. display top row: definition selector & creator
    display_unit_panel(snowsesh)

    # 3. get unique code types for filtering
    #code_types = ["All"] + list(st.session_state.codes["CODE_TYPE"].unique())

    # 4. display main row: a. code searcher & b. selected codes
    col1, col2 = st.columns([1, 1])

    #with col1:
    #    # code searcher
    #    display_code_search_panel(code_types)

    # 5. find codes from existing defintions
    #with col1:
    #    find_codes_from_existing_phenotypes()

    # 6. Show selected codes
    #with col2:
        # selected codes
    #    display_selected_codes()

main()
