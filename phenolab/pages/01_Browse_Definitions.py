import streamlit as st
from snowflake.snowpark import Session
from utils.database_utils import (
    get_aic_definitions,
    get_snowflake_session,
    get_definitions_from_snowflake_and_return_as_annotated_list_with_id_list,
    return_codes_for_given_definition_id_as_df,
)
from utils.definition_interaction_utils import (
    compare_definition_codes,
    display_definition_codes_summary,
    display_definition_metadata,
    get_missing_codes_df,
)
from utils.style_utils import set_font_lato

# # 02_Browse_Database_Definitions.py

# Enables users to browse AI Centre definitions stored in Snowflake and /
# perform side-by-side comparisons between all definitions in DEFINITIONSTORE. /
# Users can identify shared and unique codes, for validation and refinement.


# def view_aic_definitions():
#     """
#     Display AIC Definitions
#     """
#     st.title("AI Centre Definitions")

#     session = get_snowflake_session()

#     definitions = get_aic_definitions(session)
#     st.dataframe(definitions)

#     if st.button("Refresh", key="aic_refresh_button"):
#         get_aic_definitions(session)
#         st.rerun()

def view_definitions(session: Session, database: str, schema: str):
    """
    Users can select which tables they would like to view definitions from
    """
    st.title("Browse definitions")
  
    all_tables = [row["name"] for row in session.sql(f"SHOW TABLES IN SCHEMA {database}.{schema}").collect()]

    chosen_tables = st.multiselect("Select definition source", options=all_tables, default='AIC_DEFINITIONS')
    table_list_str = ', '.join([f"'{t}'" for t in chosen_tables])

    query = f"""SELECT DEFINITION_ID, DEFINITION_NAME, DEFINITION_SOURCE,
    VERSION_DATETIME, UPLOADED_DATETIME
    FROM {database}.{schema}.DEFINITIONSTORE
    WHERE SOURCE_TABLE IN ({table_list_str})
    GROUP BY DEFINITION_ID, DEFINITION_NAME, VERSION_DATETIME, UPLOADED_DATETIME, DEFINITION_SOURCE
    ORDER BY DEFINITION_NAME"""
    df = session.sql(query).to_pandas()

    st.dataframe(df)

def create_definition_panel(session,
                            column,
                            panel_name,
                            definition_ids,
                            definition_labels
                            ):
    with column:
        st.subheader(f"Definition Panel {panel_name}")

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

            codes_df = return_codes_for_given_definition_id_as_df(session, selected_id, st.session_state.config)

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
                st.dataframe(missing_codes.loc[:, ["CODE", "CODE_DESCRIPTION", "VOCABULARY"]])
                st.write(f"Total missing codes: {len(missing_codes)}")
            else:
                st.write(f"No codes from Panel {other_panel} are missing in this panel.")
        else:
            st.write("Select a definition in the other panel to see missing codes.")


def compare_definitions():
    """
    Compare definitions side-by-side
    """
    st.title("Compare Definitions")

    st.write(
        """
        Explore clinical codes within phenotype definitions with side-by-side comparison.
        Select definitions in each panel to view their codes independently.
        Summaries show shared codes, and non-overlapping codes, between definitions
        """
        )

    # get connection
    session = get_snowflake_session()

    # get all definitions
    definition_ids, definition_labels = get_definitions_from_snowflake_and_return_as_annotated_list_with_id_list(
        session, st.session_state.config)

    # show two definition panels
    left_col, right_col = st.columns(2)

    create_definition_panel(session, left_col, "A", definition_ids, definition_labels)
    create_definition_panel(session, right_col, "B", definition_ids, definition_labels)

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
            st.dataframe(shared_df.loc[:, ["CODE", "CODE_DESCRIPTION", "VOCABULARY"]])
    else:
        st.info("Select definitions in both panels to see shared analysis.")


def main():
    st.set_page_config(page_title="Browse Definitions", layout="wide", initial_sidebar_state="expanded")
    set_font_lato()

    session = get_snowflake_session()

    # create tabs for each section
    view_tab, compare_tab = st.tabs(["View Definitions", "Compare Definitions"])

    # TAB 1: LIST AIC DEFS
    with view_tab:
        # view_aic_definitions()
        view_definitions(session, "INTELLIGENCE_DEV", "AI_CENTRE_DEFINITION_LIBRARY")

    # TAB 2: COMPARE BETWEEN DEFS
    with compare_tab:
        compare_definitions()
     

if __name__ == "__main__":
    main()