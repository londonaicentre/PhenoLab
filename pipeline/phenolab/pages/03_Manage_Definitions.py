import datetime
import glob
import os
import subprocess
import sys

import pandas as pd
import streamlit as st
from dotenv import load_dotenv
from datetime import datetime

from phmlondon.definition import Definition
from phmlondon.snow_utils import SnowflakeConnection
from utils.style_utils import set_font_lato
from utils.definition_display_utils import (
    display_selected_codes,
    display_unified_code_browser,
    load_definition,
    load_definitions_list
)
import re

## TARGET FUNCTIONALITY:
## This page handles all local definition management
## User may 1. CREATE 2. EDIT 3. VIEW/UPLOAD
## Each tab uses separate temporary definition state to avoid conflicts
## TO DO:
## Deferred rendering for each tab should save a lot of performance (e.g. if tab, with tab)

#################
#### Solution to use temporary tab definition states
#################
def use_create_tab_definition():
    """
    On entry to the tab, switch to a temporaru 'create' definition state
    """
    st.session_state.current_definition = st.session_state.create_tab_definition

def save_create_tab_definition():
    """
    Save current definition back to create tab state
    """
    st.session_state.create_tab_definition = st.session_state.current_definition

def use_edit_tab_definition():
    """
    On entry to the tab, switch to a temporary 'edit' definition state
    """
    st.session_state.current_definition = st.session_state.edit_tab_definition

def save_edit_tab_definition():
    """
    Save current definition back to edit tab state
    """
    st.session_state.edit_tab_definition = st.session_state.current_definition
#################
#################


def display_definition_panel() -> str:
    """
    Display top panel
    Components for existing definition selection, and new definition creation
    Returns:
        str:
            New definition name
    """
    st.subheader("Enter definition name:")
    col1, col2 = st.columns([2, 1])

    # new definition name input
    with col1:
        new_definition_name = st.text_input("New definition name",
                label_visibility="collapsed")

        is_measurement = st.checkbox("Is Measurement", value=False,
                help="Check if this definition represents a measurement with a result value")

        with st.expander("Naming conventions:"):
            st.markdown("- Keep lowercase other than abbreviations, no spaces or special characters")
            st.markdown("- Examples: `unstable_angina_sus`, `ferritin_gp`, `schizophrenia_or_other_psychotic_disorder`")
            st.markdown("- Format as `<verbose_description_of_definition>_<code_provenance (if any)>`")
            st.markdown("- Examples of `<code_provenance>` are `gp`, `sus`. This reflects intended use and may be left blank for global definitions.")
            st.markdown("- If `Is Measurement` is selected, `<measurement>` prefix will be added *automatically*")

    # initialising this outside column block so stil returnable if button not clicked
    final_definition_name = new_definition_name

    # component: new definition button
    with col2:
        if st.button("Create definition") and new_definition_name:
           # refuse spaces
            if " " in new_definition_name:
                with col1:
                    st.error("Cannot create definition with spaces. Please use underscores instead.")
                return new_definition_name
            # refuse special characters
            if re.search(r'[^a-zA-Z0-9_]', new_definition_name):
                with col1:
                    st.error("Cannot create definition with special characters. Only letters, numbers, and underscores are permitted.")
                return new_definition_name
            # incorporate measurement prefix
            if is_measurement and not new_definition_name.startswith("measurement_"):
                final_definition_name = f"measurement_{new_definition_name}"

            st.session_state.current_definition = Definition.from_scratch(definition_name=final_definition_name)
            print(st.session_state.current_definition)
            if "used_checkbox_keys" in st.session_state:
                for checkbox_key in st.session_state.used_checkbox_keys:
                    st.session_state[checkbox_key] = False
            with col1:
                st.success(f"Created new definition: {final_definition_name}")

    st.markdown("---")

    return final_definition_name

def display_edit_definition_panel() -> str:
    """
    Display top panel
    Components for existing definition selection, and new definition creation
    Returns:
        str:
            New definition name
    """
    st.subheader("Manage Definitions")
    col1, col2 = st.columns([2, 1])

    # components: existing definition selector
    with col1:
        definitions_list = load_definitions_list()
        selected_definition_file = st.selectbox(
            "Custom definition list",
            options=definitions_list,
            label_visibility="collapsed",
            index=0 if definitions_list else None,
        )
        # component: edit button
    with col2:
        if selected_definition_file and st.button("Edit definition"):
            with st.spinner("Loading definition..."):
                file_path = os.path.join("data/definitions", selected_definition_file)
                definition = load_definition(file_path)
                if definition:
                    st.session_state.current_definition = definition
                    with col1:
                        st.success(f"Loaded definition: {definition.definition_name}")

    st.markdown("---")

def get_definitions_list():
    """
    Get list of all definition jsons from the definitions directory
    """
    definitions_dir = "data/definitions"
    if not os.path.exists(definitions_dir):
        return []

    definition_files = glob.glob(os.path.join(definitions_dir, "*.json"))
    return [os.path.basename(f) for f in definition_files]

@st.cache_data(show_spinner=False)
def display_definition_content(definition_file):
    """
    Display content from a selected definition
    """
    try:
        file_path = os.path.join("data/definitions", definition_file)
        definition = Definition.from_json(file_path)

        # definition info
        st.subheader(f"Definition: {definition.definition_name}")
        st.caption(f"ID: {definition.definition_id} | Version: {definition.definition_version}")
        st.caption(f"Source: {definition.definition_source}")

        # codelists and codes
        total_codes = 0
        for codelist in definition.codelists:
            with st.expander(f"{codelist.codelist_vocabulary.value} ({len(codelist.codes)} codes)"):
                for code in codelist.codes:
                    st.text(f"{code.code}: {code.code_description}")
                total_codes += len(codelist.codes)

        st.info(f"Total: {len(definition.codelists)} codelists, {total_codes} codes")

        return definition
    except Exception as e:
        st.error(f"Error loading definition: {e}")
        raise e

def upload_definitions_to_snowflake():
    """
    Unions all and uploads to Snowflake Definition Library
    """
    definition_files = get_definitions_list()
    if not definition_files:
        st.error("No definition files found to upload")
        return

    # connect
    with st.spinner("Connecting to Snowflake..."):
        try:
            snowsesh = SnowflakeConnection()
            snowsesh.use_database("INTELLIGENCE_DEV")
            snowsesh.use_schema("AI_CENTRE_DEFINITION_LIBRARY")

            st.success("Connected to Snowflake")
        except Exception as e:
            st.error(f"Failed to connect to Snowflake: {e}")
            return

    # upload_time = datetime.datetime.now()
    all_rows = pd.DataFrame()
    definitions_to_remove = {}
    definitions_to_add = []

    with st.spinner(f"Processing {len(definition_files)} definition files..."):
        for def_file in definition_files:
            try:
                file_path = os.path.join("data/definitions", def_file)
                definition = Definition.from_json(file_path)

                query = f"""
                SELECT DEFINITION_ID, DEFINITION_NAME, VERSION_DATETIME
                FROM AIC_DEFINITIONS
                WHERE DEFINITION_ID = '{definition.definition_id}'
                """
                existing_definition = snowsesh.execute_query_to_df(query)

                if not existing_definition.empty:
                    max_version_in_db = existing_definition["VERSION_DATETIME"].max()
                    current_version = definition.version_datetime

                    print(definition.definition_name)
                    print(current_version)
                    print(max_version_in_db)

                    if current_version == max_version_in_db:
                        st.info(f"Skipping {def_file} as it already exists in the database")
                        continue

                    if current_version < max_version_in_db:
                        st.info(f"Skipping {def_file} as a newer version {max_version_in_db} exists in the database")
                        continue

                    # otherwise, we have a newer version and should record that we want to delete the old one
                    definitions_to_remove[definition.definition_id] = [definition.definition_name, current_version]

                # put the current timestamp as the uploaded datetime
                definition.uploaded_datetime = datetime.now()

                all_rows = pd.concat([all_rows, definition.to_dataframe()])
                definitions_to_add.append(definition.definition_name)

            except Exception as e:
                st.error(f"Error processing {def_file}: {e}")
                raise e

    # upload
    if not all_rows.empty:
        with st.spinner(f"Uploading {len(all_rows)} rows to Snowflake..."):
            try:
                df = pd.DataFrame(all_rows)
                df.columns = df.columns.str.upper()
                # st.write(df)
                snowsesh.load_dataframe_to_table(df=df, table_name="AIC_DEFINITIONS", mode="append")
                st.success(f"Successfully uploaded new definitions {definitions_to_add} to the AIC definition library")

                # delete old versions
                for id, [name, current_version] in definitions_to_remove.items():
                    snowsesh.session.sql(
                            f"""DELETE FROM AIC_DEFINITIONS WHERE DEFINITION_ID = '{id}' AND
                            VERSION_DATETIME != CAST('{current_version}' AS TIMESTAMP)"""
                        ).collect()
                    st.info(f"Deleted old version(s) of {name}")

                # run update.py script to refresh DEFINITIONSTORE
                # with st.spinner("Updating DEFINITIONSTORE..."):
                #     try:
                #         current_dir = os.path.dirname(os.path.abspath(__file__))
                #         update_script_path = os.path.normpath(
                #             os.path.join(current_dir, "../../definition_library/update.py")
                #         )

                #         if not os.path.exists(update_script_path):
                #             raise FileNotFoundError(f"Update script not found at {update_script_path}")

                #         result = subprocess.run(
                #             [sys.executable, update_script_path], capture_output=True, text=True, check=True
                #         )
                #         st.success("Definition store updated successfully")
                #     except subprocess.CalledProcessError as e:
                #         st.error(f"Error updating definition store: {e.stderr}")
                #     except Exception as e:
                #         st.error(f"Error executing update script: {str(e)}")

            except Exception as e:
                st.error(f"Error uploading to Snowflake: {e}")
                raise e
    else:
        st.warning("No data to upload")

def main():
    st.set_page_config(page_title="Manage Definitions", layout="wide")
    set_font_lato()
    st.title("Manage Definitions")

    # initialise session state
    if "current_definition" not in st.session_state:
        st.session_state.current_definition = None
    if "create_tab_definition" not in st.session_state: # split create/edit to separate states
        st.session_state.create_tab_definition = None
    if "edit_tab_definition" not in st.session_state: # split create/edit to separate states
        st.session_state.edit_tab_definition = None
    if "codes" not in st.session_state:
        st.session_state.codes = None

    # confirm vocab is loaded
    if st.session_state.codes is None:
        st.warning("Please load a vocabulary from the Load Vocabulary page first.")
        return

    # create tabs for each section
    create_tab, edit_tab, view_upload_tab = st.tabs(["Create New", "Edit Existing", "View & Upload"])

    # get unique code types for filtering (used in create and edit tabs)
    code_types = ["All"] + list(st.session_state.codes["CODE_TYPE"].unique())

    ## TAB 1: CREATE DEFINITION
    with create_tab:
        # use temporary state variable for the current tab
        use_create_tab_definition()

        display_definition_panel()

        save_create_tab_definition()

        col1, col2 = st.columns([1, 1])
        with col1:
            display_unified_code_browser(code_types, key_suffix="create")
        with col2:
            display_selected_codes(key_suffix="create")

    # TAB 2: EDIT DEFINITION
    with edit_tab:
        # use temporary state variable for the current tab
        use_edit_tab_definition()

        display_edit_definition_panel()

        save_edit_tab_definition()

        col1, col2 = st.columns([1, 1])
        with col1:
            display_unified_code_browser(code_types, key_suffix="edit")
        with col2:
            display_selected_codes(key_suffix="edit")

    # TAB 3: VIEW AND UPLOAD DEFINITION
    with view_upload_tab:
        st.markdown("This page will upload all definitions to `AI_CENTRE_DEFINITION_LIBRARY.AIC_DEFINITIONS`")

        _, b, _ = st.columns(3)
        [maincol] = st.columns(1)

        definition_count = len(get_definitions_list())
        with b:
            st.text(" ")
            if definition_count > 0:
                if st.button(f"Upload new definitions to Snowflake"):
                    with maincol:
                        upload_definitions_to_snowflake()
            else:
                st.warning("No definitions available to upload")

        st.markdown("---")

        col1, col2 = st.columns([1, 1.5])

        with col1:
            st.subheader("Available Definitions")
            definition_files = get_definitions_list()

            if not definition_files:
                st.info("No definition files found. Create some definitions first.")
            else:
                selected_definition = st.selectbox("Select a definition to view", options=definition_files)

        with col2:
            if "selected_definition" in locals() and selected_definition:
                display_definition_content(selected_definition)
            else:
                st.info("Select a definition from the list to view its contents")

if __name__ == "__main__":
    main()