import streamlit as st
from utils.definition_page_display_utils import (
    display_code_search_panel,
    display_selected_codes,
    find_codes_from_existing_phenotypes,
)

from phmlondon.definition import Definition


def display_definition_panel() -> str:
    """
    Display top panel
    Compoents for existing definition selection, and new definition creation
    Returns:
        str:
            New definition name
    """
    st.subheader("Enter definition name:")
    col1, col2 = st.columns([2, 1])

    # new definition name input
    with col1:
        new_definition_name = st.text_input("New definition name", label_visibility="collapsed")

    # component: new definition button
    with col2:
        if st.button("Create definition") and new_definition_name:
            st.session_state.current_definition = Definition.from_scratch(definition_name=new_definition_name)
            st.session_state.selected_codes = []
            with col1:
                st.success(f"Created new definition: {new_definition_name}")

    st.markdown("---")

    return new_definition_name


def main():
    st.set_page_config(page_title="Create a new definition", layout="wide")
    st.title("Create a new definition")

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


main()
