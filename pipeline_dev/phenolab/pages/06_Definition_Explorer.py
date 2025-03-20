# see all codes in a DEFINITION, 
# compare two lists

import streamlit as st
from dotenv import load_dotenv

import re

from phmlondon.snow_utils import SnowflakeConnection

load_dotenv()

if "snowsesh" not in st.session_state:
    st.session_state.snowsesh = SnowflakeConnection()
    st.session_state.snowsesh.use_database("INTELLIGENCE_DEV")
    st.session_state.snowsesh.use_schema("AI_CENTRE_DEFINITION_LIBRARY")

snowsesh = st.session_state.snowsesh

def search_boxes():

    st.title("Phenotype Code Explorer")
    st.write(
        """
        Explore clinical codes within phenotype definitions. Select a source, phenotype, and
        codelist to view the included codes.
        """
    )

    # DROPDOWN ONE: SOURCE LIBRARY
    source_query = """
    SELECT DISTINCT DEFINITION_SOURCE
    FROM DEFINITIONSTORE
    ORDER BY DEFINITION_SOURCE
    """
    sources = snowsesh.execute_query_to_df(source_query)
    source_options = ["Select", "All"] + sources["DEFINITION_SOURCE"].tolist()
    selected_source = st.selectbox("Select Phenotype Source:", source_options)

    # dynamic query creation based on what sources are selected
    where_clause = "WHERE 1=1 "
    if selected_source != "Select":
        if selected_source != "All":
            where_clause += f"AND DEFINITION_SOURCE = '{selected_source}'"

    # DROPDOWN TWO: PHENOTYPE
    phenotype_query = f"""
    SELECT DISTINCT DEFINITION_NAME
    FROM DEFINITIONSTORE
    {where_clause}
    ORDER BY DEFINITION_NAME
    """
    phenotypes = snowsesh.execute_query_to_df(phenotype_query)
    phenotype_options = ["Select", "All"] + phenotypes["DEFINITION_NAME"].tolist()
    selected_phenotype = st.selectbox("Select DEFINITION:", phenotype_options)
    if selected_phenotype != "Select":
        if selected_phenotype != "All":
            where_clause += f" AND DEFINITION_NAME = '{selected_phenotype}'"

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
            st.write("Phenotype IDs:")
            st.dataframe(codes_df.loc[:, ["DEFINITION_ID", "CODELIST_VERSION"]].drop_duplicates())
            st.dataframe(codes_df.loc[:, ["CODE", "CODE_DESCRIPTION", "VOCABULARY"]])
        else:
            st.write("No codes found for the selected criteria.")

def compare_2_definitions():
    st.title("Definition Comparer")
    st.write("Compare two defintions:")
    comparison_query = f"""
    SELECT DISTINCT DEFINITION_SOURCE, DEFINITION_ID, DEFINITION_NAME
    FROM DEFINITIONSTORE
    ORDER BY DEFINITION_NAME
    """
    comparison_defintions = snowsesh.execute_query_to_df(comparison_query)

    list_of_all_definitions = [f"[{row['DEFINITION_SOURCE']}] [{row['DEFINITION_ID']}] {row['DEFINITION_NAME']}" 
                                for i, row in comparison_defintions.iterrows()]

    selected_for_comparison = st.multiselect(label="Choose two definitions to compare", 
                                            options=list_of_all_definitions,
                                            max_selections=2)
    if selected_for_comparison:
        if len(selected_for_comparison) <2:
            st.write("Please select two definitions to compare")
        elif len(selected_for_comparison) >2:
            st.write("Please select a maximum of two definitions to compare")
        elif len(selected_for_comparison) == 2:
            print(selected_for_comparison)

            definition_ids = [re.match(r"\[[^\]]+\] \[([^\]]+)\]", selected_for_comparison[i]).group(1) 
                            for i in range(2)]
            print(definition_ids)

            lists_of_codes_for_comparison = []
            for definition_id in definition_ids:
                codes_as_list = snowsesh.session.sql(f"""
                SELECT DISTINCT
                    CODE,
                    CODE_DESCRIPTION,
                    VOCABULARY,
                    DEFINITION_ID,
                    CODELIST_VERSION
                FROM INTELLIGENCE_DEV.AI_CENTRE_DEFINITION_LIBRARY.DEFINITIONSTORE
                WHERE DEFINITION_ID = '{definition_id}'
                ORDER BY VOCABULARY, CODE
                """).collect()
                codes_as_set = set(codes_as_list)
                lists_of_codes_for_comparison.append(codes_as_set)
                # st.write(codes_as_list)

            shared_codes = lists_of_codes_for_comparison[0] & lists_of_codes_for_comparison[1]
            list_1_only_codes = lists_of_codes_for_comparison[0] - lists_of_codes_for_comparison[1]
            list_2_only_codes = lists_of_codes_for_comparison[1] - lists_of_codes_for_comparison[0]

            st.write("Shared Codes:")
            st.write(list(shared_codes))
            st.markdown(f"Codes in **{selected_for_comparison[0]}** only:")
            st.write(list(list_1_only_codes))
            st.markdown(f"Codes in **{selected_for_comparison[1]}** only:")    
            st.write(list(list_2_only_codes))

search_boxes()
compare_2_definitions()
