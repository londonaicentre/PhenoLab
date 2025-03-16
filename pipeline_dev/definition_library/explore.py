"""
Script to run a streamlit app which allows browsing of the stored definitions via dropdown menus
"""

import streamlit as st
from dotenv import load_dotenv

from phenolab.snow_utils import SnowflakeConnection


def main():
    load_dotenv()

    if "snowsesh" not in st.session_state:
        st.session_state.snowsesh = SnowflakeConnection()
        st.session_state.snowsesh.use_database("INTELLIGENCE_DEV")
        st.session_state.snowsesh.use_schema("AI_CENTRE_DEFINITION_LIBRARY")

    snowsesh = st.session_state.snowsesh

    st.title("Definition Code Explorer")
    st.write(
        """
        Explore clinical codes within definition definitions. Select a source, definition, and
        codelist to view the included codes.
        """
    )

    # DROPDOWN ONE: SOURCE LIBRARY
    source_query = """
    SELECT DISTINCT SOURCE_SYSTEM
    FROM DEFINITIONSTORE
    ORDER BY SOURCE_SYSTEM
    """
    sources = snowsesh.execute_query_to_df(source_query)
    source_options = ["Select", "All"] + sources["SOURCE_SYSTEM"].tolist()
    selected_source = st.selectbox("Select Definition Source:", source_options)

    # dynamic query creation based on what sources are selected
    where_clause = "WHERE 1=1 "
    if selected_source != "Select":
        if selected_source != "All":
            where_clause += f"AND SOURCE_SYSTEM = '{selected_source}'"

    # DROPDOWN TWO: DEFINITION
    definition_query = f"""
    SELECT DISTINCT DEFINITION_NAME
    FROM DEFINITIONSTORE
    {where_clause}
    ORDER BY DEFINITION_NAME
    """
    definitions = snowsesh.execute_query_to_df(definition_query)
    definition_options = ["Select", "All"] + definitions["DEFINITION_NAME"].tolist()
    selected_definition = st.selectbox("Select Definition:", definition_options)
    if selected_definition != "Select":
        if selected_definition != "All":
            where_clause += f" AND DEFINITION_NAME = '{selected_definition}'"

    # DROPDOWN THREE: CODELIST
    codelist_query = f"""
    SELECT DISTINCT CODELIST_NAME
    FROM DEFINITIONSTORE
    {where_clause}
    ORDER BY CODELIST_NAME
    """
    codelists = snowsesh.execute_query_to_df(codelist_query)
    codelist_options = ["Select", "All"] + codelists["CODELIST_NAME"].tolist()
    selected_codelist = st.selectbox("Select Codelist:", codelist_options)

    # Show dataframe
    if selected_codelist != "Select":
        final_where = where_clause
        if selected_codelist != "All":
            final_where += f" AND CODELIST_NAME = '{selected_codelist}'"

        codes_query = f"""
        SELECT DISTINCT
            CODE,
            CODE_DESCRIPTION,
            VOCABULARY,
            DEFINITION_ID,
            CODELIST_VERSION
        FROM INTELLIGENCE_DEV.AI_CENTRE_DEFINITION_LIBRARY.DEFINITIONSTORE
        {final_where}
        ORDER BY VOCABULARY, CODE
        """

        codes_df = snowsesh.execute_query_to_df(codes_query)

        if not codes_df.empty:
            st.write("Selected Codes:")
            st.write(f"Total codes: {len(codes_df)}")
            st.write("Definition IDs:")
            st.dataframe(codes_df.loc[:, ["DEFINITION_ID", "CODELIST_VERSION"]].drop_duplicates())
            st.dataframe(codes_df.loc[:, ["CODE", "CODE_DESCRIPTION", "VOCABULARY"]])
        else:
            st.write("No codes found for the selected criteria.")


if __name__ == "__main__":
    main()
