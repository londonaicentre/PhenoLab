import streamlit as st
import pandas as pd
from dotenv import load_dotenv
from phmlondon.snow_utils import SnowflakeConnection

def main():
    load_dotenv()

    if 'snowsesh' not in st.session_state:
        st.session_state.snowsesh = SnowflakeConnection()
        st.session_state.snowsesh.use_database("INTELLIGENCE_DEV")
        st.session_state.snowsesh.use_schema("AI_CENTRE_PHENOTYPE_LIBRARY")

    snowsesh = st.session_state.snowsesh

    st.title("Phenotype Code Explorer")
    st.write(
        """
        Explore clinical codes within phenotype definitions. Select a source, phenotype, and codelist to view the included codes.
        """
    )

    # DROPDOWN ONE: SOURCE LIBRARY
    source_query = """
    SELECT DISTINCT PHENOTYPE_SOURCE
    FROM PHENOSTORE
    ORDER BY PHENOTYPE_SOURCE
    """
    sources = snowsesh.execute_query_to_df(source_query)
    source_options = ['Select', 'All'] + sources['PHENOTYPE_SOURCE'].tolist()
    selected_source = st.selectbox('Select Phenotype Source:', source_options)

    # dynamic query creation based on what sources are selected
    where_clause = "WHERE 1=1 "
    if selected_source != 'Select':
        if selected_source != 'All':
            where_clause += f"AND PHENOTYPE_SOURCE = '{selected_source}'"

    # DROPDOWN TWO: PHENOTYPE
    phenotype_query = f"""
    SELECT DISTINCT PHENOTYPE_NAME
    FROM PHENOSTORE
    {where_clause}
    ORDER BY PHENOTYPE_NAME
    """
    phenotypes = snowsesh.execute_query_to_df(phenotype_query)
    phenotype_options = ['Select', 'All'] + phenotypes['PHENOTYPE_NAME'].tolist()
    selected_phenotype = st.selectbox('Select Phenotype:', phenotype_options)
    if selected_phenotype != 'Select':
        if selected_phenotype != 'All':
            where_clause += f" AND PHENOTYPE_NAME = '{selected_phenotype}'"

    # DROPDOWN THREE: CODELIST
    codelist_query = f"""
    SELECT DISTINCT CODELIST_NAME
    FROM PHENOSTORE
    {where_clause}
    ORDER BY CODELIST_NAME
    """
    codelists = snowsesh.execute_query_to_df(codelist_query)
    codelist_options = ['Select', 'All'] + codelists['CODELIST_NAME'].tolist()
    selected_codelist = st.selectbox('Select Codelist:', codelist_options)

    # Show dataframe
    if selected_codelist != 'Select':
        final_where = where_clause
        if selected_codelist != 'All':
            final_where += f" AND CODELIST_NAME = '{selected_codelist}'"

        codes_query = f"""
        SELECT DISTINCT
            CODE,
            CODE_DESCRIPTION,
            VOCABULARY
        FROM PHENOSTORE
        {final_where}
        ORDER BY VOCABULARY, CODE
        """

        codes_df = snowsesh.execute_query_to_df(codes_query)

        if not codes_df.empty:
            st.write(f"Selected Codes:")
            st.write(f"Total codes: {len(codes_df)}")
            st.dataframe(codes_df)
        else:
            st.write("No codes found for the selected criteria.")

if __name__ == "__main__":
    main()