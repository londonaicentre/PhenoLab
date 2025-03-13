import os

import pandas as pd
import streamlit as st
from dotenv import load_dotenv
from utils.data_utils import Code, Definition


def load_definitions():
    """List all definition files in the definitions directory"""
    definition_files = []
    try:
        if os.path.exists("data/definitions"):
            definition_files = [f for f in os.listdir("data/definitions") if f.endswith(".json")]
    except Exception as e:
        st.error(f"Error listing definition files: {e}")

    return definition_files


def get_concept_types(df):
    """Get unique concept types from the dataframe"""
    if df is not None and 'CONCEPT_TYPE' in df.columns:
        return ['All'] + sorted(df['CONCEPT_TYPE'].unique().tolist())
    return ['All']


def filter_concepts(df, search_term, concept_type):
    """Filter concepts based on search term and concept type"""
    if df is None:
        return pd.DataFrame()

    filtered_df = df.copy()

    # Filter by search term
    if search_term:
        search_term = search_term.lower()
        filtered_df = filtered_df[
            filtered_df['CONCEPT_NAME'].str.lower().str.contains(search_term) |
            filtered_df['CONCEPT_CODE'].str.lower().str.contains(search_term)
        ]

    # Filter by concept type
    if concept_type and concept_type != 'All':
        filtered_df = filtered_df[filtered_df['CONCEPT_TYPE'] == concept_type]

    return filtered_df


def main():
    st.set_page_config(page_title="Definitions Creator", layout="wide")
    st.title("Definitions Creator")

    load_dotenv()

    # Initialize session state variables
    if "concepts" not in st.session_state:
        st.session_state.concepts = None

    if "current_definition" not in st.session_state:
        st.session_state.current_definition = None

    if "selected_codes" not in st.session_state:
        st.session_state.selected_codes = []

    if "definition_type" not in st.session_state:
        st.session_state.definition_type = "observation"

    # Info section about definitions
    with st.expander("What is a definition?", expanded=False):
        st.markdown("""
        * **Definition** = a collection of Codelist(s) that define a simple concept
        * **Codelist** = a set of Codes in the same vocabulary
        * **Code** = a single Code representing a discrete clinical concept

        E.g., a Definition of Hypertension could contain a READV2 Codelist and a SNOMED Codelist,
        each containing a list of codes related to hypertension.
        """)

    # Panel 1 - Definition Management (First Row)
    st.header("Definition Management")

    col1, col2 = st.columns([1, 1])

    with col1:
        # List existing definitions
        definition_files = load_definitions()
        selected_definition_file = st.selectbox(
            "Custom definition list",
            options=definition_files,
            index=0 if definition_files else None
        )

        if selected_definition_file and st.button("Edit definition"):
            with st.spinner("Loading definition..."):
                try:
                    file_path = os.path.join("data/definitions", selected_definition_file)
                    st.session_state.current_definition = Definition.load_from_file(file_path)
                    st.success(f"Loaded definition: {st.session_state.current_definition.definition_name}")
                except Exception as e:
                    st.error(f"Error loading definition: {e}")

    with col2:
        # Create new definition
        new_definition_name = st.text_input("New definition name")

        definition_types = ["observation", "medication", "disorder", "procedure"]
        selected_type = st.selectbox("Definition type", options=definition_types)

        if new_definition_name and st.button("Create new definition"):
            if st.session_state.current_definition is not None:
                should_replace = st.warning("This will replace your currently loaded definition. Continue?")
                if not should_replace:
                    st.stop()

            st.session_state.current_definition = Definition(
                definition_name=new_definition_name,
                definition_type=selected_type
            )
            st.session_state.selected_codes = []
            st.success(f"Created new definition: {new_definition_name}")

    # Second row for panels 2 and 3
    if st.session_state.concepts is None:
        st.warning("Please load a concept list from the Concept List Creator page first.")
    else:
        st.header("Concept Selection")

        col1, col2 = st.columns([1, 1])

        # Panel 2 - Concept Selection (Left, Second Row)
        with col1:
            st.subheader("Filter and Select Concepts")

            # Search box
            search_term = st.text_input("Filter concepts", "")

            # Concept type filter
            concept_types = get_concept_types(st.session_state.concepts)
            concept_type = st.selectbox("Concept type", options=concept_types)

            # Apply filters
            filtered_df = filter_concepts(st.session_state.concepts, search_term, concept_type)

            # Show filtered results
            if not filtered_df.empty:
                # Create a dataframe with select boxes
                st.write(f"Showing {len(filtered_df)} matching concepts")

                # Create pagination
                items_per_page = 20
                total_pages = (len(filtered_df) + items_per_page - 1) // items_per_page

                if "page" not in st.session_state:
                    st.session_state.page = 0

                # Add pagination controls
                col1, col2, col3 = st.columns([1, 3, 1])

                with col1:
                    if st.button("Previous") and st.session_state.page > 0:
                        st.session_state.page -= 1

                with col2:
                    st.write(f"Page {st.session_state.page + 1} of {max(1, total_pages)}")

                with col3:
                    if st.button("Next") and st.session_state.page < total_pages - 1:
                        st.session_state.page += 1

                # Get current page of data
                start_idx = st.session_state.page * items_per_page
                end_idx = min(start_idx + items_per_page, len(filtered_df))
                page_df = filtered_df.iloc[start_idx:end_idx].reset_index(drop=True)

                # Create a selection interface for the concepts
                for idx, row in page_df.iterrows():
                    col1, col2 = st.columns([4, 1])

                    with col1:
                        st.write(f"**{row['CONCEPT_NAME']}** ({row['CONCEPT_CODE']}) - {row['VOCABULARY']}")
                        st.write(f"Count: {row['CONCEPT_COUNT']} | Type: {row['CONCEPT_TYPE']}")

                    with col2:
                        # Create a unique key for each button
                        button_key = f"select_{row['CONCEPT_CODE']}_{row['VOCABULARY']}"

                        # Check if this code is already in selected_codes
                        is_selected = any(
                            c.code == row['CONCEPT_CODE'] and
                            c.vocabulary == row['VOCABULARY']
                            for c in st.session_state.selected_codes
                        )

                        if is_selected:
                            if st.button("Remove", key=button_key):
                                # Remove code from selected_codes
                                st.session_state.selected_codes = [
                                    c for c in st.session_state.selected_codes
                                    if not (c.code == row['CONCEPT_CODE'] and c.vocabulary == row['VOCABULARY'])
                                ]

                                # If there's a current definition, also remove from there
                                if st.session_state.current_definition:
                                    code_to_remove = Code(
                                        code=row['CONCEPT_CODE'],
                                        code_description=row['CONCEPT_NAME'],
                                        vocabulary=row['VOCABULARY']
                                    )
                                    st.session_state.current_definition.remove_code(code_to_remove)
                        else:
                            if st.button("Add", key=button_key):
                                # Create a Code object and add it to selected_codes
                                new_code = Code(
                                    code=row['CONCEPT_CODE'],
                                    code_description=row['CONCEPT_NAME'],
                                    vocabulary=row['VOCABULARY']
                                )
                                st.session_state.selected_codes.append(new_code)

                                # If there's a current definition, also add there
                                if st.session_state.current_definition:
                                    st.session_state.current_definition.add_code(new_code)

                    st.markdown("---")
            else:
                st.info("No concepts match your search criteria.")

        # Panel 3 - Selected Concepts (Right, Second Row)
        with col2:
            st.subheader("Selected Concepts")

            if st.session_state.current_definition:
                st.write(f"Current Definition: **{st.session_state.current_definition.definition_name}** ({st.session_state.current_definition.definition_type})")

                # Show all codes in the current definition
                all_codes = []
                for vocabulary, codelist in st.session_state.current_definition.codelists.items():
                    all_codes.extend(codelist.codes)

                # Sort codes by vocabulary and then by description
                all_codes.sort(key=lambda x: (x.vocabulary, x.code_description))

                if all_codes:
                    # Display selected codes organized by vocabulary
                    vocabularies = set(code.vocabulary for code in all_codes)

                    for vocabulary in sorted(vocabularies):
                        vocab_codes = [code for code in all_codes if code.vocabulary == vocabulary]

                        with st.expander(f"{vocabulary} ({len(vocab_codes)} codes)", expanded=True):
                            for code in vocab_codes:
                                col1, col2 = st.columns([4, 1])

                                with col1:
                                    st.write(f"**{code.code_description}** ({code.code})")

                                with col2:
                                    # Create a unique key for each remove button
                                    remove_key = f"remove_{code.code}_{vocabulary}"

                                    if st.button("Remove", key=remove_key):
                                        # Remove from current definition
                                        st.session_state.current_definition.remove_code(code)

                                        # Remove from selected_codes too
                                        st.session_state.selected_codes = [
                                            c for c in st.session_state.selected_codes
                                            if not (c.code == code.code and c.vocabulary == code.vocabulary)
                                        ]

                                st.markdown("---")

                    # Save button
                    if st.button("Save Definition"):
                        try:
                            if not all_codes:
                                st.warning("The definition doesn't have any codes. Add some codes before saving.")
                            else:
                                filepath = st.session_state.current_definition.save_to_file()
                                st.success(f"Saved definition to {os.path.basename(filepath)}")
                        except Exception as e:
                            st.error(f"Error saving definition: {e}")
                else:
                    st.info("No codes have been added to this definition yet. Use the filters on the left to find and add codes.")
            else:
                st.info("No definition is currently loaded or created. Either select an existing definition to edit or create a new one.")


if __name__ == "__main__":
    main()
