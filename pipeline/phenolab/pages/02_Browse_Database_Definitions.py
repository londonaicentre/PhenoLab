"""
This script is used to explore clinical codes within phenotype definitions, with side-by-side comparison.
"""

import re

import streamlit as st
from utils.database_utils import (
    connect_to_snowflake,
    get_data_from_snowflake_to_dataframe,
    get_definitions_from_snowflake_and_return_as_annotated_list_with_id_list,
)

# DEFINITIONS PANEL
def create_definition_panel(snowsesh,
                            column,
                            panel_name,
                            definition_ids,
                            definition_labels
                            ):
    with column:
        st.subheader(f"Definition Panel {panel_name}")

        # 1. select definition
        selected_definition = st.selectbox(
            f"Select Definition ({panel_name})",
            options=definition_labels,
            key=f"def_select_{panel_name}"
        )

        if selected_definition:
            # get codes for selected definition
            selected_id = definition_ids[definition_labels.index(selected_definition)]

            codes_query = f"""
            SELECT DISTINCT
                CODE,
                CODE_DESCRIPTION,
                VOCABULARY,
                DEFINITION_ID,
                CODELIST_VERSION
            FROM INTELLIGENCE_DEV.AI_CENTRE_DEFINITION_LIBRARY.DEFINITIONSTORE
            WHERE DEFINITION_ID = '{selected_id}'
            ORDER BY VOCABULARY, CODE
            """

            codes_df = get_data_from_snowflake_to_dataframe(snowsesh, codes_query)

            if not codes_df.empty:
                st.write("Definition details:")
                st.dataframe(codes_df.loc[:, ["DEFINITION_ID", "CODELIST_VERSION", "VOCABULARY"]].drop_duplicates())

                # Display codes
                st.write("Codes:")
                st.dataframe(codes_df.loc[:, ["CODE", "CODE_DESCRIPTION", "VOCABULARY"]])
                st.write(f"Total codes: {len(codes_df)}")

                # Store codes in session state for comparison
                st.session_state[f"codes_{panel_name}"] = codes_df
            
            else:
                st.write("No codes found for the selected definition.")

def show_missing_codes(column, panel_name):
    # Missing codes section
    with column:

        if f"codes_{'B' if panel_name == 'A' else 'A'}" in st.session_state:
            other_panel = 'B' if panel_name == 'A' else 'A'
            other_codes = st.session_state[f"codes_{other_panel}"]

            # Find codes in the other panel that are missing from this one
            st.session_state["missing_codes"] = other_codes[~other_codes["CODE"].isin(
                st.session_state[f"codes_{panel_name}"]["CODE"])]

            if not st.session_state["missing_codes"].empty:
                st.write(f"Codes in Panel {other_panel} missing from this panel:")
                st.dataframe(st.session_state["missing_codes"].loc[:, ["CODE", "CODE_DESCRIPTION", "VOCABULARY"]])
                st.write(f"Total missing codes: {len(st.session_state['missing_codes'])}")
            else:
                st.write(f"No codes from Panel {other_panel} are missing in this panel.")
        else:
            st.write("Select a definition in the other panel to see missing codes.")


def main():
    # Main page
    snowsesh = connect_to_snowflake()

    st.title("Browse database definitions")
    st.write(
        """
        Explore clinical codes within phenotype definitions with side-by-side comparison.
        Select definitions in each panel to view their codes independently.
        Summaries show shared codes, and non-overlapping codes, between definitions
        """
        )

    # get all definitions
    definition_ids, definition_labels = get_definitions_from_snowflake_and_return_as_annotated_list_with_id_list(snowsesh)

    # show two definition panels
    left_col, right_col = st.columns(2)

    create_definition_panel(snowsesh, left_col, "A", definition_ids, definition_labels)
    create_definition_panel(snowsesh, right_col, "B", definition_ids, definition_labels)

    col2, _ = st.columns(2)
    with col2:
        st.subheader("Missing codes")
    
    col3A, col3B = st.columns(2)

    show_missing_codes(col3A, "A")
    show_missing_codes(col3B, "B")

    # show analysis section
    st.divider()
    st.subheader("Shared Analysis")

    # ...only if both panels have selections
    if "codes_A" in st.session_state and "codes_B" in st.session_state:
        codes_A = set(st.session_state["codes_A"]["CODE"])
        codes_B = set(st.session_state["codes_B"]["CODE"])

        shared_codes = codes_A.intersection(codes_B)
        only_A = codes_A.difference(codes_B)
        only_B = codes_B.difference(codes_A)

        col1, col2, col3 = st.columns(3)

        with col1:
            st.metric("Shared Codes", len(shared_codes))

        with col2:
            st.metric("Only in Panel A", len(only_A))

        with col3:
            st.metric("Only in Panel B", len(only_B))

        # hiding shared codes unless specifically selected
        if shared_codes and st.checkbox("Show shared codes"):
            shared_df = st.session_state["codes_A"][st.session_state["codes_A"]["CODE"].isin(shared_codes)]
            st.dataframe(shared_df.loc[:, ["CODE", "CODE_DESCRIPTION", "VOCABULARY"]])
    else:
        st.info("Select definitions in both panels to see shared analysis.")


if __name__ == "__main__":
    main()
