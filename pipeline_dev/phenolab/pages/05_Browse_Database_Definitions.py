"""
This script is used to explore clinical codes within phenotype definitions.
"""

import streamlit as st
import re

from utils.database_utils import connect_to_snowflake, get_data_from_snowflake_to_dataframe, get_definitions_from_snowflake_and_return_as_annotated_list_with_id_list

# Main page
snowsesh = connect_to_snowflake()

st.title("Browse database definitions")
st.write(
    """
    Explore clinical codes within phenotype definitions. Select a source, phenotype, and
    codelist to view the included codes. Start typing to search.
    """
)

# DROPDOWN ONE: SOURCE LIBRARY
source_query = """
SELECT DISTINCT DEFINITION_SOURCE
FROM INTELLIGENCE_DEV.AI_CENTRE_DEFINITION_LIBRARY.DEFINITIONSTORE
ORDER BY DEFINITION_SOURCE
"""
# sources = snowsesh.execute_query_to_df(source_query)
sources = get_data_from_snowflake_to_dataframe(snowsesh, source_query)
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
FROM INTELLIGENCE_DEV.AI_CENTRE_DEFINITION_LIBRARY.DEFINITIONSTORE
{where_clause}
ORDER BY DEFINITION_NAME
"""
# phenotypes = snowsesh.execute_query_to_df(phenotype_query)
phenotypes = get_data_from_snowflake_to_dataframe(snowsesh, phenotype_query)
phenotype_options = ["Select", "All"] + phenotypes["DEFINITION_NAME"].tolist()
selected_phenotype = st.selectbox("Select DEFINITION:", phenotype_options)
if selected_phenotype != "Select":
    if selected_phenotype != "All":
        where_clause += f" AND DEFINITION_NAME = '{selected_phenotype}'"

# DROPDOWN THREE: CODELIST
codelist_query = f"""
SELECT DISTINCT CODELIST_NAME
FROM INTELLIGENCE_DEV.AI_CENTRE_DEFINITION_LIBRARY.DEFINITIONSTORE
{where_clause}
ORDER BY CODELIST_NAME
"""
# codelists = snowsesh.execute_query_to_df(codelist_query)
codelists = get_data_from_snowflake_to_dataframe(snowsesh, codelist_query)
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
    # codes_df = snowsesh.execute_query_to_df(codes_query)
    codes_df = get_data_from_snowflake_to_dataframe(snowsesh, codes_query)

    if not codes_df.empty:
        st.write("Selected Codes:")
        st.write(f"Total codes: {len(codes_df)}")
        st.write("Phenotype IDs:")
        st.dataframe(codes_df.loc[:, ["DEFINITION_ID", "CODELIST_VERSION"]].drop_duplicates())
        st.dataframe(codes_df.loc[:, ["CODE", "CODE_DESCRIPTION", "VOCABULARY"]])
    else:
        st.write("No codes found for the selected criteria.")

st.divider()

# Second part of the page
st.title("Compare definitions")
st.write("Choose two definitions to compare:")

_, list_of_all_definitions = get_definitions_from_snowflake_and_return_as_annotated_list_with_id_list(snowsesh)

selected_for_comparison = st.multiselect(label="Choose two definitions to compare", 
                                        options=list_of_all_definitions,
                                        label_visibility="collapsed",
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
        full_code_data_for_comparison_display = []
        for definition_id in definition_ids:
            query = f"""
            SELECT DISTINCT
                CODE,
                CODE_DESCRIPTION,
                VOCABULARY,
                DEFINITION_ID,
                CODELIST_VERSION
            FROM INTELLIGENCE_DEV.AI_CENTRE_DEFINITION_LIBRARY.DEFINITIONSTORE
            WHERE DEFINITION_ID = '{definition_id}'
            ORDER BY VOCABULARY, CODE
            """
            # codes_as_list = get_data_from_snowflake_to_list(snowsesh, query)
            codes_as_df = get_data_from_snowflake_to_dataframe(snowsesh, query)
            full_code_data_for_comparison_display.append(codes_as_df)
            # codes_as_list = snowsesh.session.sql(f"""
            # SELECT DISTINCT
            #     CODE,
            #     CODE_DESCRIPTION,
            #     VOCABULARY,
            #     DEFINITION_ID,
            #     CODELIST_VERSION
            # FROM INTELLIGENCE_DEV.AI_CENTRE_DEFINITION_LIBRARY.DEFINITIONSTORE
            # WHERE DEFINITION_ID = '{definition_id}'
            # ORDER BY VOCABULARY, CODE
            # """).collect()

            # print(codes_as_list)
            # codes_as_set = set(codes_as_list)
            # lists_of_codes_for_comparison.append(codes_as_set)
            lists_of_codes_for_comparison.append(set(codes_as_df['CODE']))
            
            # st.write(codes_as_list)

        shared_codes = lists_of_codes_for_comparison[0] & lists_of_codes_for_comparison[1]
        list_1_only_codes = lists_of_codes_for_comparison[0] - lists_of_codes_for_comparison[1]
        list_2_only_codes = lists_of_codes_for_comparison[1] - lists_of_codes_for_comparison[0]

        st.markdown(f'**Comparing {selected_for_comparison[0]} and {selected_for_comparison[1]}**')
        st.write("Shared Codes:")
        # st.write(list(shared_codes))
        st.dataframe(full_code_data_for_comparison_display[0].loc
                    [full_code_data_for_comparison_display[0]['CODE'].isin(list(shared_codes))])
        st.markdown(f"Codes in **{selected_for_comparison[0]}** only:")
        # st.write(list(list_1_only_codes))
        st.dataframe(full_code_data_for_comparison_display[0].loc
                    [full_code_data_for_comparison_display[0]['CODE'].isin(list(list_1_only_codes))])
        st.markdown(f"Codes in **{selected_for_comparison[1]}** only:")    
        # st.write(list(list_2_only_codes))
        st.dataframe(full_code_data_for_comparison_display[1].loc
                    [full_code_data_for_comparison_display[1]['CODE'].isin(list(list_2_only_codes))])