import streamlit as st
import re
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