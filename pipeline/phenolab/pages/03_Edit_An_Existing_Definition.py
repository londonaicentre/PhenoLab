import os
import sys

import streamlit as st

from utils.definition_page_display_utils import load_definitions_list, load_definition, display_code_search_panel, display_selected_codes, find_codes_from_existing_phenotypes

def display_definition_panel() -> str:
    """
    Display top panel
    Compoents for existing definition selection, and new definition creation
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

                    # load definition codes into the session state for tracking
                    st.session_state.selected_codes = []
                    for codelist in definition.codelists.values():
                        for code in codelist.codes:
                            st.session_state.selected_codes.append(code)

    st.markdown("---")

def main():
    st.set_page_config(page_title="Edit an existing definition", layout="wide")
    st.title("Edit an existing definition")

    # state variables
    ## the definition that is loaded (or created) and currently being worked on
    if "current_definition" not in st.session_state:
        st.session_state.current_definition = None
    ## actively selected codes that are part of the current definition
    if "selected_codes" not in st.session_state:
        st.session_state.selected_codes = []
    ## all codes in source data pulled in from selector
    if "codes" not in st.session_state:
        st.session_state.codes = None

    # 1. check if codes are loaded
    if st.session_state.codes is None:
        st.warning("Please load a vocabulary from the Load Vocabulary page first.")
        return

    # 2. display top row: definition selector & creator
    display_definition_panel()

    # 3. get unique code types for filtering
    code_types = ["All"] + list(st.session_state.codes["CODE_TYPE"].unique())

    # 4. display main row: a. code searcher & b. selected codes
    col1, col2 = st.columns([1, 1])

    with col1:
        # code searcher
        display_code_search_panel(code_types)

    with col2:
        # selected codes
        display_selected_codes()

    # 5. find codes from existing defintions
    with col1:
        find_codes_from_existing_phenotypes()

if __name__ == "__main__":
    main()
