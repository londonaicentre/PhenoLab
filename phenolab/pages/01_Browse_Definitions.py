import streamlit as st

from utils.database_utils import (
    get_snowflake_session,
    get_definitions_from_snowflake_and_return_as_annotated_list_with_id_list,
    return_codes_for_given_definition_id_as_df,
)
from utils.definition_interaction_utils import (
    compare_definition_codes,
    display_definition_codes_summary,
    display_definition_metadata,
    get_missing_codes_df,
    display_codes_in_selected_definition_simply,
)
from utils.style_utils import set_font_lato
from utils.config_utils import load_config

# # 02_Browse_Database_Definitions.py

# Enables users to browse AI Centre definitions stored in Snowflake and /
# perform side-by-side comparisons between all definitions in DEFINITIONSTORE. /
# Users can identify shared and unique codes, for validation and refinement.

def view_definitions():
    """
    Users can select which tables they would like to view definitions from
    """

    all_tables = [row["name"] for row in st.session_state.session.sql(
        f"""SHOW TABLES IN SCHEMA {st.session_state.config["definition_library"]["database"]}.
            {st.session_state.config["definition_library"]["schema"]}""").collect()]

    chosen_tables = st.multiselect("Select definition source", options=all_tables, default='AIC_DEFINITIONS',
                    placeholder="Select a definition source", label_visibility="collapsed",)
    if chosen_tables:
        table_list_str = ', '.join([f"'{t}'" for t in chosen_tables])
        query = f"""SELECT DEFINITION_ID, DEFINITION_NAME, DEFINITION_VERSION, DEFINITION_SOURCE,
        VERSION_DATETIME, UPLOADED_DATETIME
        FROM {st.session_state.config["definition_library"]["database"]}.
            {st.session_state.config["definition_library"]["schema"]}.DEFINITIONSTORE
        WHERE SOURCE_TABLE IN ({table_list_str})
        GROUP BY DEFINITION_ID, DEFINITION_NAME, DEFINITION_VERSION, VERSION_DATETIME, UPLOADED_DATETIME, DEFINITION_SOURCE
        ORDER BY DEFINITION_NAME"""
        df = st.session_state.session.sql(query).to_pandas()

        st.text(" ")
        # st.dataframe(df, hide_index=True)
        selected_def = st.dataframe(df, key="data", on_select="rerun", selection_mode="single-row", hide_index=True,)
        if selected_def:
            selected_id = df['DEFINITION_ID'].iloc[selected_def["selection"]["rows"]].values[0]
            codes_df = return_codes_for_given_definition_id_as_df(selected_id)
            st.write("")
            st.write("")
            display_codes_in_selected_definition_simply(codes_df)

def create_definition_panel(column,
                            panel_name,
                            definition_ids,
                            definition_labels
                            ):
    with column:
        # st.subheader(f"Definition Panel {panel_name}")

        # select definition
        selected_definition = st.selectbox(
            f"Select Definition ({panel_name})",
            options=definition_labels,
            key=f"def_select_{panel_name}",
            label_visibility="hidden",
        )

        if selected_definition:
            # get codes for selected definition
            selected_id = definition_ids[definition_labels.index(selected_definition)]

            codes_df = return_codes_for_given_definition_id_as_df(selected_id)

            display_definition_metadata(codes_df)
            display_definition_codes_summary(codes_df)

            # Store codes in session state for comparison
            if not codes_df.empty:
                st.session_state[f"codes_{panel_name}"] = codes_df


def show_missing_codes(column, panel_name):
    # missing codes section
    with column:

        if f"codes_{'B' if panel_name == 'A' else 'A'}" in st.session_state:
            other_panel = 'B' if panel_name == 'A' else 'A'
            other_codes = st.session_state[f"codes_{other_panel}"]

            # find codes in the other panel that are missing from this one
            missing_codes = get_missing_codes_df(st.session_state[f"codes_{panel_name}"], other_codes)

            if not missing_codes.empty:
                st.write(f"Codes in Panel {other_panel} missing from this panel:")
                st.dataframe(missing_codes.loc[:, ["CODE", "CODE_DESCRIPTION", "VOCABULARY"]], hide_index=True)
                st.write(f"Total missing codes: {len(missing_codes)}")
            else:
                st.write(f"No codes from Panel {other_panel} are missing in this panel.")
        else:
            st.write("Select a definition in the other panel to see missing codes.")


def compare_definitions():
    """
    Compare definitions side-by-side
    """
    # st.title("Compare Definitions")

    st.write(
        """
        Explore clinical codes within definitions with side-by-side comparison.
        Select definitions in each panel to view their codes.
        Summaries show shared codes, and non-overlapping codes between the two selected definitions
        """
        )

    # get all definitions
    definition_ids, definition_labels = get_definitions_from_snowflake_and_return_as_annotated_list_with_id_list()

    # show two definition panels
    left_col, right_col = st.columns(2)

    create_definition_panel(left_col, "A", definition_ids, definition_labels)
    create_definition_panel(right_col, "B", definition_ids, definition_labels)

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
        comparison = compare_definition_codes(st.session_state["codes_A"], st.session_state["codes_B"])

        col1, col2, col3 = st.columns(3)

        with col1:
            st.metric("Shared Codes", comparison["shared_count"])

        with col2:
            st.metric("Only in Panel A", comparison["only_in_1_count"])

        with col3:
            st.metric("Only in Panel B", comparison["only_in_2_count"])

        # hiding shared codes unless specifically selected
        if comparison["shared"] and st.checkbox("Show shared codes", key="show_shared_codes_checkbox"):
            shared_df = st.session_state["codes_A"][st.session_state["codes_A"]["CODE"].isin(comparison["shared"])]
            st.dataframe(shared_df.loc[:, ["CODE", "CODE_DESCRIPTION", "VOCABULARY"]], hide_index=True)
    else:
        st.info("Select definitions in both panels to see shared analysis.")


def main():
    st.set_page_config(page_title="Browse Definitions", layout="wide", initial_sidebar_state="expanded")
    set_font_lato()
    if "session" not in st.session_state:
        st.session_state.session = get_snowflake_session()
    if "config" not in st.session_state:
        st.session_state.config = load_config()

    st.title("Browse and compare definitions")

    # create tabs for each section
    view_tab, search_tab, compare_tab = st.tabs(["View Definitions", "Search Definitions", "Compare Definitions"])

    # TAB 1: LIST AIC DEFS
    with view_tab:
        # view_aic_definitions()
        view_definitions()

    # TAB 2: SEARCH DEFINITIONS
    with search_tab:
        st.write("")
        definition_ids, definition_labels = get_definitions_from_snowflake_and_return_as_annotated_list_with_id_list()
        selected_definition = st.selectbox("Search for a definition", options=definition_labels, 
            label_visibility="visible",placeholder="Start typing to search for a definition", index=None)
        if selected_definition:
            selected_id = definition_ids[definition_labels.index(selected_definition)]

            codes_df = return_codes_for_given_definition_id_as_df(selected_id)
            st.write("")
            st.write("")
            display_codes_in_selected_definition_simply(codes_df)


    # TAB 3: COMPARE BETWEEN DEFS
    with compare_tab:
        compare_definitions()
     

if __name__ == "__main__":
    main()