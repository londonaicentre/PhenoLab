import os
import re

import streamlit as st

from utils.definition_interaction_utils import (
    display_definition_from_file,
    display_selected_codes,
    display_unified_code_browser,
    load_definition,
    load_definitions_list,
    update_aic_definitions_table
)
from utils.database_utils import get_snowflake_session
from utils.style_utils import set_font_lato
from utils.config_utils import load_config, preload_vocabulary
from utils.definition import Definition

# # 03_Manage_Definitions.py

# Comprehensive interface for 1. creating, 2. editing and 3. viewing clinical /
# code definitions. Users can search across multiple vocabularies, add /
# codes to definitions, and upload definitions to Snowflake. /

# TO DO
# - Can we implement deferred tab rendering?
# - E.g. if tab, with tab...


##################
# Temp tab states #
##################
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
            st.markdown("- Format as `<verbose_description_of_definition>_<vocabular(ies)>`")
            st.markdown("- Examples: `unstable_angina_ICD10`, `ferritin_SNOMED`, `schizophrenia_or_other_psychotic_disorder_SNOMEDICD10`")
            st.markdown("- Vocabulary ordering convention is SNOMED > ICD10 > OPCS4 > Other(s)")
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
        selected_definition = st.selectbox(
            "Custom definition list",
            options=definitions_list,
            label_visibility="collapsed",
            index=0 if definitions_list else None,
        )
        # component: edit button
    with col2:
        if selected_definition and st.button("Edit definition"):
            with st.spinner("Loading definition..."):
                if st.session_state.config["local_development"]:
                    selected_definition = os.path.join("data/definitions", selected_definition)
                definition = load_definition(selected_definition)
                if definition:
                    st.session_state.current_definition = definition
                    with col1:
                        st.success(f"Loaded definition: {definition.definition_name}")

    st.markdown("---")

def main():
    st.set_page_config(page_title="Manage Definitions", layout="wide")
    set_font_lato()
    if "config" not in st.session_state:
        st.session_state.config = load_config()
    if "codes" not in st.session_state:
        preload_vocabulary()
    if "session" not in st.session_state:
        st.session_state.session = get_snowflake_session()
    st.title("Manage Definitions")
    # load_dotenv()

    # initialise session state
    if "current_definition" not in st.session_state:
        st.session_state.current_definition = None
    if "create_tab_definition" not in st.session_state: # split create/edit to separate states
        st.session_state.create_tab_definition = None
    if "edit_tab_definition" not in st.session_state: # split create/edit to separate states
        st.session_state.edit_tab_definition = None

    # confirm vocab is loaded
    if st.session_state.codes is None:
        st.warning("Please load a vocabulary from the Load Vocabulary page first.")
        return

    # create tabs for each section
    if st.session_state.config["local_development"]:
    # upload tab is for pushing jsons to Snowflake - for local development only
        create_tab, edit_tab, view_upload_tab = st.tabs(["Create New", "Edit Existing", "View & Upload"])
    else:
        create_tab, edit_tab = st.tabs(["Create New", "Edit Existing"])

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
            display_unified_code_browser(code_types, st.session_state.config, key_suffix="create")
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
            display_unified_code_browser(code_types, st.session_state.config, key_suffix="edit")
        with col2:
            display_selected_codes(key_suffix="edit")

    # TAB 3: VIEW AND UPLOAD DEFINITION
    if st.session_state.config["local_development"]:
        with view_upload_tab:
            col1, col2 = st.columns([1, 1.5])

            with col1:
                st.subheader("Available Definitions")
                definition_files = load_definitions_list()

                if not definition_files:
                    st.info("No definition files found. Create some definitions first.")
                else:
                    selected_definition = st.selectbox("Select a definition to view", options=definition_files)

            with col2:
                if "selected_definition" in locals() and selected_definition:
                    display_definition_from_file(selected_definition)
                else:
                    st.info("Select a definition from the list to view its contents")

            st.markdown("---")

            st.markdown(f"This will upload all definitions to `{st.session_state.config['definition_library']['database']}.{st.session_state.config['definition_library']['schema']}.AIC_DEFINITIONS` and refresh `DEFINITIONSTORE`." \
            "Updated definitions will overwrite previous versions.")
            _, b, _ = st.columns(3)
            [maincol] = st.columns(1)

            definition_count = len(load_definitions_list())
            with b:
                st.text(" ")
                if definition_count > 0:
                    if st.button("Upload new / updated definitions to Snowflake"):
                        with maincol:
                            update_aic_definitions_table( 
                                database=st.session_state.config["definition_library"]["database"], 
                                schema=st.session_state.config["definition_library"]["schema"])
                else:
                    st.warning("No definitions available to upload")

if __name__ == "__main__":
    main()