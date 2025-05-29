"""
Script to run a streamlit app which visualises some data quality bits
"""
import os
import re

import numpy as np
import pandas as pd
import plotly.express as px
import streamlit as st
import utils.helper_functions as hf
from dotenv import load_dotenv
from utils.database_utils import (
    get_definition_sources,
    get_definitions,
    get_existing_units,
    get_existing_units_from_cleaned,
    get_unit_distributions,
    get_unit_info,
)
from utils.definition_page_display_utils import (
    display_code_search_panel,
    display_selected_codes,
    find_codes_from_existing_phenotypes,
    load_definition,
    load_definitions_list,
)

from phmlondon.snow_utils import SnowflakeConnection


def display_unit_panel(_connection: SnowflakeConnection) -> str:
    """
    Display top panel
    Components for existing unit selection, and new unit creation
    Returns:
        str:
            New unit name
    """
    st.subheader("Enter standardised unit:")
    col1, col2, col3 = st.columns([1, 1, 1])

    # load in definitions
    with col1:
        selected_definition_source = st.selectbox(
            "Definition Source",
            options=get_definition_sources(_connection),
            #label_visibility="collapsed"
        )

    with col2:
        definitions_list = get_definitions(selected_definition_source, _connection)
        selected_definition = st.selectbox(
            "Definition list",
            options=definitions_list,
            #label_visibility="collapsed",
            index=0 if definitions_list else None,
        )

    # new unit name input
    with col3:
        existing_units = get_existing_units(selected_definition,
                                            _connection)
        existing_unit_name = st.selectbox(
            'Existing Units for this definition',
            options = existing_units
        )
        if 'current_units' not in st.session_state:
            st.session_state.current_units = existing_unit_name

    unit_df = get_unit_info(selected_definition, _connection)

    #Check if there is any data:
    if unit_df.shape[1] > 0:
        unit_df.columns = ['Units',
                        'Count',
                        'Mean',
                        'Median',
                        'Minimum',
                        'Maximum']

        #Pull out where these are already in the data
        existing_result_value_units = get_existing_units_from_cleaned(st.session_state.current_units,
                                                                      selected_definition,
                                                                      _connection)

        unit_df["plotter"] = False
        unit_df.plotter[unit_df.Units.isin(existing_result_value_units)] = True

        edited_unit_df = st.data_editor(
            unit_df,
            width = 1000,
            column_config={
                "plotter": st.column_config.CheckboxColumn(
                    "Already in list",
                    help="Select units to plot distribution",
                    default=False,
                    )
                    },
            disabled=["widgets"],
            hide_index=True,
        )

        with st.form('unit_distributions'):
            st.markdown('### Plot the distributions of the units')
            col1, col2, col3, col4 = st.columns(4)
            local_max = edited_unit_df.Maximum[edited_unit_df.plotter].max()
            local_min = edited_unit_df.Minimum[edited_unit_df.plotter].min()

            with col1:
                xmin = st.number_input('XMin',
                                    value = local_min)

            with col2:
                xmax = st.number_input('XMax',
                                    value = local_max)

            with col3:
                nbinsx = st.number_input('Bins/unit',
                                         value = 1.0,
                                         step = 0.01)

            with col4:
                unit_submit = st.form_submit_button()

            if unit_submit:
                checked_units = edited_unit_df.Units[edited_unit_df.plotter].to_list()

                #Now pull out the values for the checked boxes
                unit_distr_df = get_unit_distributions(selected_definition, checked_units, _connection)

                #Limit this by the values as this is slowing things down
                above_min = np.where(unit_distr_df.RESULT_VALUE >= xmin)
                below_max = np.where(unit_distr_df.RESULT_VALUE <= xmax)

                unit_distr_plot = px.histogram(
                    unit_distr_df.loc[np.intersect1d(above_min, below_max), :],
                    x='RESULT_VALUE',
                    color = 'RESULT_VALUE_UNITS',
                    range_x=[xmin, xmax],
                    nbins = round((xmax - xmin)*nbinsx),
                    marginal= 'violin'
                    #nbins=died_bins,  # Slider from above,
                    )
                st.plotly_chart(unit_distr_plot)

        with st.form('add_to_json'):
            st.markdown(f'### Add these units to \'{st.session_state.current_unit}\'')
            st.write(f'Current definition: {selected_definition}')

            #Now show the units we have checked
            if sum(edited_unit_df.plotter) > 0:
                cols_to_include = [col for col in edited_unit_df.columns if col != 'plotter']
                units_to_include_df = edited_unit_df.loc[edited_unit_df.plotter, cols_to_include]
                st.dataframe(units_to_include_df, width= 1000)

            else:
                st.write('No units selected')

            #Now add them to the list of units
            add_units_submit = st.form_submit_button('Add these units to this new overarching unit')
            if add_units_submit and st.session_state.current_unit is not None:
                if not st.session_state.units_old:

                    #Make table of what we have so far
                    unit_columns = ['RESULT_VALUE_UNITS', 'CLEANED_UNITS', 'DEFINITION_NAME']
                    new_table = pd.DataFrame({unit_columns[0]: units_to_include_df.Units,
                                            unit_columns[1]: st.session_state.current_unit,
                                            unit_columns[2]: selected_definition
                                            })
                    #st.dataframe(new_table)#.loc[:,unit_columns])

                    #Compare what is in the json to what we have already in the table
                    json_filename = '../table_generation/unit_standardisation/unit_conversions.json'
                    current_table = pd.read_json(json_filename)
                    current_table['already_present'] = True
                    combined_tabs = pd.merge(new_table,
                                    current_table,
                                    on = unit_columns,
                                    how = 'outer'
                                    )
                    new_units_size = combined_tabs.shape[0] -  current_table.shape[0]
                    if not new_units_size:
                        st.warning('No new units to add')
                    else:
                        combined_tabs[unit_columns].to_json(json_filename)
                        st.success(f'{str(new_units_size)} units corresponding to {st.session_state.current_unit} saved!')
                else:
                    st.warning(f'Unable to add units to {st.session_state.current_unit} as already exists')

    else:
        st.warning('No data to present')

    st.markdown("---")

    #Probably just read in the bits, check the boxes (don't do Joes thing, over complex and very similar?) and then add to the json

    return existing_unit_name


def main():
    st.set_page_config(page_title="Create a new standardised unit", layout="wide")
    st.title("Create a new unit")

    if "snowsesh" not in st.session_state:
        load_dotenv()
        db = 'INTELLIGENCE_DEV'
        schema = "AI_CENTRE_PHENOTYPE_LIBRARY"
        st.session_state.snowsesh = SnowflakeConnection()

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
    display_unit_panel(st.session_state.snowsesh)

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
