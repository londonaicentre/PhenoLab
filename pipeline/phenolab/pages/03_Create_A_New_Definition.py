import streamlit as st
from utils.definition_display_utils import (
    display_selected_codes,
    display_unified_code_browser
)

from phmlondon.definition import Definition
from utils.style_utils import set_font_lato

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
        new_definition_name = st.text_input("New definition name", label_visibility="collapsed")

    # component: new definition button
    with col2:
        if st.button("Create definition") and new_definition_name:
            st.session_state.current_definition = Definition.from_scratch(definition_name=new_definition_name)
            print(st.session_state.current_definition)
            if "used_checkbox_keys" in st.session_state:
                for checkbox_key in st.session_state.used_checkbox_keys:
                    st.session_state[checkbox_key] = False
            with col1:
                st.success(f"Created new definition: {new_definition_name}")

    st.markdown("---")

    return new_definition_name


def main():
    st.set_page_config(page_title="Create a new definition", layout="wide")
    set_font_lato()
    st.title("Create a new definition")

    # state variables
    ## the definition that is loaded (or created) and currently being worked on
    if "current_definition" not in st.session_state:
        st.session_state.current_definition = None

    ## the vocab
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

    # 4. display main row: a. unified code browser & b. selected codes
    col1, col2 = st.columns([1, 1])

    with col1:
        display_unified_code_browser(code_types)

    # 5. Show selected codes
    with col2:
        display_selected_codes()

if __name__ == "__main__":
    main()