import os
import sys
from typing import List, Optional, Tuple

import pandas as pd
import streamlit as st

if os.path.dirname(os.path.abspath(__file__)) not in sys.path:
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from utils.data_utils import Code, Definition, load_definition_from_json

# HELPER FUNCTIONS

def load_definitions_list() -> List[str]:
    """
    Get list of definition files from /data/definitions
    """
    definitions_list = []
    try:
        if os.path.exists("data/definitions"):
            definitions_list = [f for f in os.listdir("data/definitions") if f.endswith(".json")]
    except Exception as e:
        st.error(f"Unable to list definition files: {e}")

    return definitions_list


def load_definition(file_path: str) -> Optional[Definition]:
    """
    Load definition from json
    """
    try:
        definition = load_definition_from_json(file_path)
        return definition
    except Exception as e:
        st.error(f"Unable to load definition: {e}")
        return None


def filter_concepts(df: pd.DataFrame, search_term: str, concept_type: str) -> pd.DataFrame:
    """
    Filter concepts dataframe based on search term and concept type

    Args:
        df(pd.DataFrame):
            concept dataframe held in state from the concept list selector
        search_term(str):
            Term in search term box
        concept_type(str):
            Type selected from drop down (e.g. OBSERVATION)
    """
    filtered_df = df.copy()

    # apply filter
    if concept_type and concept_type != "All":
        filtered_df = filtered_df[filtered_df["CONCEPT_TYPE"] == concept_type]

    # apply search term
    if search_term:
        # make case insensitive
        filtered_df = filtered_df[
            filtered_df["CONCEPT_NAME"].str.contains(search_term, case=False)
            | filtered_df["CONCEPT_CODE"].str.contains(search_term, case=False)
        ]

    return filtered_df.sort_values("CONCEPT_COUNT", ascending=False)


def create_code_from_row(row: pd.Series) -> Code:
    """
    Create a Code object from dataframe row
    """
    return Code(
        code=row["CONCEPT_CODE"], code_description=row["CONCEPT_NAME"], vocabulary=row["VOCABULARY"]
    )

# STREAMLIT FUNCTIONS

def display_definition_panel() -> str:
    """
    Display top panel
    Compoents for existing definition selection, and new definition creation
    Returns:
        str:
            New definition name
    """
    st.subheader("Manage Definitions")
    col1, col2, col3 = st.columns([2, 1, 1])

    # components: existing definition selector
    with col1:
        definitions_list = load_definitions_list()
        selected_definition_file = st.selectbox(
            "Custom definition list",
            options=definitions_list,
            index=0 if definitions_list else None,
        )
        # component: edit button
        if selected_definition_file and st.button("Edit definition"):
            with st.spinner("Loading definition..."):
                file_path = os.path.join("data/definitions", selected_definition_file)
                definition = load_definition(file_path)
                if definition:
                    st.session_state.current_definition = definition
                    st.success(f"Loaded definition: {definition.definition_name}")

                    # load definition codes into the session state for tracking
                    st.session_state.selected_concepts = []
                    for codelist in definition.codelists.values():
                        for code in codelist.codes:
                            st.session_state.selected_concepts.append(code)

    # new definition name input
    with col2:
        new_definition_name = st.text_input("New definition name")

    # component: new definition button
    with col3:
        if new_definition_name and st.button("Create new definition"):
            st.session_state.current_definition = Definition(
                definition_name=new_definition_name, definition_type="OBSERVATION"
                # change when additional types
            )
            st.session_state.selected_concepts = []
            st.success(f"Created new definition: {new_definition_name}")

    st.markdown("---")

    return new_definition_name


def display_concept_search_panel(concept_types: List[str]) -> Tuple[pd.DataFrame, str, str]:
    st.subheader("Find Concepts")

    # FIXED CONTAINER
    with st.container(height=150):
        search_col1, search_col2 = st.columns([3, 1])

        # components: search inputs
        with search_col1:
            search_term = st.text_input("Filter concepts")

        with search_col2:
            concept_type = st.selectbox("Concept type", options=concept_types)

        # Filter the concepts
        filtered_concepts = filter_concepts(st.session_state.concepts, search_term, concept_type)
        if not filtered_concepts.empty:
            st.write(f"Found {len(filtered_concepts)} concepts")
        else:
            st.info("No concepts found matching the search criteria")
            return filtered_concepts, search_term, concept_type

    # SCROLLING CONTAINER
    with st.container(height=450):
        if not filtered_concepts.empty:
            st.write(f"Found {len(filtered_concepts)} concepts")
            for idx, row in filtered_concepts.head(1000).iterrows():
                col1a, col1b = st.columns([4, 1])
                with col1a:
                    st.text(f"{row['CONCEPT_NAME']} ({row['VOCABULARY']})")

                    # display summary stats
                    basic_info = []
                    if 'CONCEPT_CODE' in row and pd.notna(row['CONCEPT_CODE']):
                        basic_info.append(f"Code: {row['CONCEPT_CODE']}")

                    if 'CONCEPT_COUNT' in row and pd.notna(row['CONCEPT_COUNT']):
                        basic_info.append(f"Count: {row['CONCEPT_COUNT']}")

                    if 'MEDIAN_AGE' in row and pd.notna(row['MEDIAN_AGE']):
                        basic_info.append(f"MedianAge: {row['MEDIAN_AGE']:.1f}")

                    if 'MEDIAN_VALUE' in row and pd.notna(row['MEDIAN_VALUE']):
                        # change label per concept type
                        if row['CONCEPT_TYPE'] in ['SUS_APC', 'SUS_APC_PROC']:
                            basic_info.append(f"MedianLOS: {row['MEDIAN_VALUE']:.1f}")
                        else:
                            basic_info.append(f"MedianValue: {row['MEDIAN_VALUE']:.1f}")

                    if 'PERCENT_HAS_RESULT_VALUE' in row and pd.notna(row['PERCENT_HAS_RESULT_VALUE']):
                        basic_info.append(f"Has Result: {row['PERCENT_HAS_RESULT_VALUE']:.1f}%")

                    if basic_info:
                        st.caption(" | ".join(basic_info))

                with col1b:
                    is_selected = any(
                        c.code == row["CONCEPT_CODE"] and c.vocabulary == row["VOCABULARY"]
                        for c in st.session_state.selected_concepts
                    )

                    if not is_selected:
                        if st.button("Add", key=f"add_{idx}"):
                            code = create_code_from_row(row)
                            st.session_state.selected_concepts.append(code)
                            if st.session_state.current_definition:
                                st.session_state.current_definition.add_code(code)
                            st.rerun()
        else:
            st.info("No concepts found matching the search criteria")

    return filtered_concepts, search_term, concept_type


def display_selected_concepts():
    """
    Display the selected concepts panel (right panel)
    """
    st.subheader("Selected Concepts")

    # FIXED CONTAINER
    with st.container(height=150):
        # current definition information
        if st.session_state.current_definition:
            definition = st.session_state.current_definition
            st.write(f"Definition: **{definition.definition_name}**")
            st.caption(f"Type: {definition.definition_type} | ID: {definition.definition_id}")

            # component: save button
            if st.button("Save Definition"):
                try:
                    filepath = definition.save_to_json()
                    st.success(f"Definition saved to: {filepath}")
                except Exception as e:
                    st.error(f"Error saving definition: {e}")
        else:
            st.info("Create or load a definition first.")

    # SCROLLING CONTAINER
    with st.container(height=450):
        if st.session_state.selected_concepts:
            # grouping by vocab (i.e. codelist)
            concepts_by_vocab = {}
            for code in st.session_state.selected_concepts:
                if code.vocabulary not in concepts_by_vocab:
                    concepts_by_vocab[code.vocabulary] = []
                concepts_by_vocab[code.vocabulary].append(code)

            for vocabulary, codes in concepts_by_vocab.items():
                st.write(f"**{vocabulary}** ({len(codes)} codes)")
                for idx, code in enumerate(codes):
                    col2a, col2b = st.columns([4, 1])
                    with col2a:
                        st.text(f"{code.code_description}")
                        st.caption(f"Code: {code.code}")

                    with col2b:
                        if st.button("Remove", key=f"remove_{vocabulary}_{idx}"):
                            st.session_state.selected_concepts.remove(code)
                            if st.session_state.current_definition:
                                st.session_state.current_definition.remove_code(code)
                            st.rerun()

                st.markdown("---")
        elif st.session_state.current_definition:
            st.info("No concepts selected. Find and add concepts with the search panel.")


def main():
    st.set_page_config(page_title="Definition Creator", layout="wide")
    st.title("Definition Creator")

    # state variables
    ## the definition that is loaded (or created) and currently being worked on
    if "current_definition" not in st.session_state:
        st.session_state.current_definition = None
    ## actively selected codes that are part of the current definition
    if "selected_concepts" not in st.session_state:
        st.session_state.selected_concepts = []
    ## all concepts in source data pulled in from selector
    if "concepts" not in st.session_state:
        st.session_state.concepts = None

    # 1. check if concepts are loaded
    if st.session_state.concepts is None:
        st.warning("Please load a concept list from the Concept List Creator page first.")
        return

    # 2. display top row: definition selector & creator
    display_definition_panel()

    # 3. get unique concept types for filtering
    concept_types = ["All"] + list(st.session_state.concepts["CONCEPT_TYPE"].unique())

    # 4. display main row: a. concept searcher & b. selected concepts
    col1, col2 = st.columns([1, 1])

    with col1:
        # concept searcher
        display_concept_search_panel(concept_types)

    with col2:
        # selected concepts
        display_selected_concepts()


if __name__ == "__main__":
    main()
