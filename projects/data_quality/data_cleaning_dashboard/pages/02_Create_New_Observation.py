"""
Script to run a streamlit app which visualises some data quality bits
"""
import json

import streamlit as st
from dotenv import load_dotenv
from utils.database_utils import (
    get_definition_sources,
    get_definitions,
    get_unit_info,
)

from phmlondon.snow_utils import SnowflakeConnection


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

    #Read in the json
    with open('table_generation/unit_standardisation/standardisations.json', 'r+') as file:
        config = json.load(file)

    # Generate the new json for the config file
    st.markdown("""###Enter standardised unit:
                Input details of new units here, including unit, observation """)
    col1, col2, col3 = st.columns([2, 1, 1])

    # new unit name input
    with col1:
        observation_name = st.text_input("New observation name",
                                        label_visibility="collapsed",
                                        placeholder='Observation name here (e.g. haemoglobin)')
        comment = st.text_input("Comment",
                                label_visibility='collapsed',
                                placeholder="""Comment about this definition e.g. table to standardise unit conversions - this one does g/l and g/dl for haemoglobin""") #noqa

    # load in definitions
    with col2:
        selected_definition_source = st.selectbox(
            "Definition Source",
            options=get_definition_sources(st.session_state.snowsesh),
            label_visibility="collapsed"
        )
        definitions_list = get_definitions(selected_definition_source, st.session_state.snowsesh)
        selected_definition = st.selectbox(
            "Custom definition list",
            options=definitions_list,
            label_visibility="collapsed",
            index=0 if definitions_list else None,
        )

    # component: new observation name
    with col3:

        if st.button("Create unit") and observation_name:
            st.session_state.observation_name = observation_name
            if observation_name in config.keys():
                current_config = config[observation_name]
            st.success(f'Generated new observation: {observation_name}')

    unit_df = get_unit_info(selected_definition, st.session_state.snowsesh)

    #Check if there is any data:
    if unit_df.shape[1] > 0:
        unit_df.columns = ['Units',
                        'Count',
                        'Mean',
                        'Median',
                        'Minimum',
                        'Maximum']

        st.dataframe(unit_df,
                     width = 1000,)

    else:
        st.write(f'No observations in {selected_definition}, choose another one')

    if observation_name not in config.keys() and observation_name:
        st.session_state.observation_name = observation_name
        print(st.session_state.observation_name)

    #Make sure that they know they might be overwriting
    elif observation_name in config.keys() and not observation_name:
        st.warning(f"""{st.session_state.observation_name} already exists - editing existing observation.
                    If you click submit it will overwrite the original config.""")
        cmetric1, cmetric2 = st.columns(2)
        with cmetric1:
            st.metric('Observation Name',
                        current_config['observation_name'])

        with cmetric2:
            st.metric('Current Definition Name',
                        current_config['definition'])

        st.write(f'Current comment: {current_config['comment']}')

    else:
        st.write('Input a definition')

    with st.form('create_config'):
        submit_json = st.form_submit_button('Save to JSON')
        if submit_json:

            #Save the config
            current_config = {'schema': 'AI_CENTRE_OBSERVATION_STAGING_TABLES',
                                'unit_table': 'UNIT_LOOKUP',
                                'observation_name': st.session_state.observation_name,
                                'definition': selected_definition,
                                'comment': comment}
            config[observation_name] = current_config

            #Write it to file
            with open('table_generation/unit_standardisation/standardisations.json', 'w') as file:
                json.dump(config, file)

            st.success(f"New observation saved: {st.session_state.observation}")

main()
