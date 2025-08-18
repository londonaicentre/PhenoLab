import os
from contextlib import nullcontext
from datetime import datetime
from typing import List, Optional, Tuple

import pandas as pd
import streamlit as st
from snowflake.snowpark import Session
from utils.database_utils import (
    get_definitions_from_snowflake_and_return_as_annotated_list_with_id_list,
    return_codes_for_given_definition_id_as_df,
)
from utils.definition import Code, Definition, VocabularyType
from utils.style_utils import container_object_with_height_if_possible

"""
# definition_display_utils.py

Utilities for loading and displaying clinical definitions in the UI.

Provides functions to:
- load definitions from json files
- display and search them in the Streamlit UI
- manage code selection when creating definitions
"""

# @st.cache_data(ttl=300)
def load_definitions_list() -> List[str]:
    """
    Get list of definition files from /data/definitions
    """
    if st.session_state.config["local_development"]:
        return load_definitions_list_from_local_files()
    else:
        return load_definitions_list_from_icb_table()

def load_definitions_list_from_local_files() -> List[str]:
    """
    Get list of definition files from /data/definitions.
    """
    if os.path.exists("data/definitions"):
        return sorted([f for f in os.listdir("data/definitions") if f.endswith(".json")])


def load_definitions_list_from_icb_table() -> List[str]:
    """
    Get list of definition versions from the ICB_DEFINITIONS table in Snowflake.
    """
    query = f"""
        SELECT DEFINITION_VERSION
        FROM {st.session_state.config["definition_library"]["database"]}.
        {st.session_state.config["definition_library"]["schema"]}.ICB_DEFINITIONS
        GROUP BY DEFINITION_ID, DEFINITION_NAME, DEFINITION_VERSION, VERSION_DATETIME, UPLOADED_DATETIME, DEFINITION_SOURCE
        ORDER BY DEFINITION_NAME;
        """
    df = st.session_state.session.sql(query).to_pandas()
    return df["DEFINITION_VERSION"].tolist()

def load_definition(file_path_or_definition_name: str) -> Optional[Definition]:
    """
    Switches between loading a definition form a local JSON or from the snowflake table according to the config
    Args:
        file_path_or_definition_name(str):
            If local development, this is the path to the JSON file.
            If remote, this is the definition version name to load from Snowflake.
    Returns:
        Definition object or None if not found
    """
    if st.session_state.config["local_development"]:
        return load_local_definition(file_path_or_definition_name)
    else:
        return load_remote_definition(file_path_or_definition_name)

def load_local_definition(file_path: str) -> Optional[Definition]:
    """
    Load definition from json
    """
    return Definition.from_json(file_path)

def load_remote_definition(definition_version_name: str) -> Optional[Definition]:
    """
    Load definition from Snowflake
    """
    query = f"""SELECT * FROM {st.session_state.config["definition_library"]["database"]}.
    {st.session_state.config["definition_library"]["schema"]}.DEFINITIONSTORE
    WHERE DEFINITION_VERSION = '{definition_version_name}';"""
    df = st.session_state.session.sql(query).to_pandas()
    df.columns = df.columns.str.lower()
    return Definition.from_dataframe(df)

def create_code_from_row(row: pd.Series) -> Code:
    """
    Create a Code object from dataframe row
    """
    vocabulary_as_enum = VocabularyType(row["VOCABULARY"])
    return Code(code=row["CODE"], code_description=row["CODE_DESCRIPTION"], code_vocabulary=vocabulary_as_enum)


def code_selected(row: pd.Series) -> bool:
    """
    Check if a code is already selected in the current definition
    """
    if st.session_state.current_definition is None:
        return False

    return any(c.code == row["CODE"] and c.code_vocabulary == VocabularyType(row["VOCABULARY"])
            for c in st.session_state.current_definition.codes)

def display_code_and_checkbox(row: pd.Series, checkbox_key: str, key_suffix=""):
    """
    Display a code with a checkbox for selection/deselection
    """
    if st.session_state.current_definition is not None:
        is_selected = code_selected(row)
    else:
        is_selected = False

    suff_checkbox_key = f"{checkbox_key}_{key_suffix}" # key suffix for shared components on same page
    checkbox_ticked = st.checkbox(
            "Any", value=is_selected, key=suff_checkbox_key, label_visibility="collapsed")

    # Need to keep track of all checkboxes so can reset them all if a new definition is created
    if "used_checkbox_keys" not in st.session_state:
        st.session_state.used_checkbox_keys = set()
    st.session_state.used_checkbox_keys.add(suff_checkbox_key)

    code = create_code_from_row(row)

    if not is_selected:
        if checkbox_ticked:
            if st.session_state.current_definition:
                st.session_state.current_definition.add_code(code)
    elif is_selected and not checkbox_ticked:
            if st.session_state.current_definition:
                st.session_state.current_definition.remove_code(code)


def parse_search_query(query):
    """
    Parse a search query to identify logical operators and extract terms.
    Supports patterns:
    - (term1) AND (term2)
    - (term1) OR (term2)
    - (term1) NOT (term2)
    - (term) - single term in parentheses
    - term - single term without parentheses
    """
    result = {"operator": None, "terms": []}

    query = query.strip()

    if not query:
        return result

    # check for logical operators
    if " AND " in query:
        result["operator"] = "AND"
        parts = query.split(" AND ")
    elif " OR " in query:
        result["operator"] = "OR"
        parts = query.split(" OR ")
    elif " NOT " in query:
        result["operator"] = "NOT"
        parts = query.split(" NOT ")
    else:
        # no operator, treat as single term
        result["terms"] = [query]
        return result

    # extract terms
    for part in parts:
        part = part.strip()
        # remove brackets
        if part.startswith("(") and part.endswith(")"):
            part = part[1:-1].strip()
        result["terms"].append(part)

    return result


def apply_search_filters(df, parsed_query, search_columns=["CODE_DESCRIPTION", "CODE"]):
    """
    Apply filters to the DataFrame based on parsed search query.

    Args:
        df (pd.DataFrame):
            DataFrame to filter
        parsed_query (dict):
            Result from parse_search_query containing 'operator' and 'terms'
        search_columns (list):
            Columns to search in (will be CODE_DESCRIPTION and CODE)

    Returns:
        pd.DataFrame:
            Filtered DataFrame!
    """
    if not parsed_query["terms"]:
        return df

    operator = parsed_query["operator"]
    terms = parsed_query["terms"]

    # workhorse function for applying filter
    def _term_filter(df, term):
        mask = pd.Series(False, index=df.index)
        for col in search_columns:
            mask = mask | df[col].str.contains(term, case=False, na=False)
        return mask

    # single term search only
    if operator is None:
        return df[_term_filter(df, terms[0])]

    # AND
    elif operator == "AND":
        mask = _term_filter(df, terms[0])
        for term in terms[1:]:
            mask = mask & _term_filter(df, term)
        return df[mask]

    # OR
    elif operator == "OR":
        mask = _term_filter(df, terms[0])
        for term in terms[1:]:
            mask = mask | _term_filter(df, term)
        return df[mask]

    # FIRST term, EXCLUDE if second term
    elif operator == "NOT":
        if len(terms) >= 2:
            mask = _term_filter(df, terms[0]) & ~_term_filter(df, terms[1])
            return df[mask]
        else:
            # handle edge case where only one term provided with NOT then treat as single term
            return df[_term_filter(df, terms[0])]

    # otherwise return original
    return df

@st.cache_data(ttl=1800, max_entries=10)  # cache for 30mins
def filter_codes(df: pd.DataFrame, search_term: str, code_type: str) -> pd.DataFrame:
    """
    Filter codes dataframe based on search term and code type

    Args:
        df(pd.DataFrame):
            code dataframe held in state from the code list selector
        search_term(str):
            Term in search term box (supports logical operators AND, OR, NOT)
        code_type(str):
            Type selected from drop down (e.g. OBSERVATION)
    """
    filtered_df = df.copy()

    # apply type filter first
    if code_type and code_type != "All":
        filtered_df = filtered_df[filtered_df["CODE_TYPE"] == code_type]

    # apply search term logic
    if search_term:
        parsed_query = parse_search_query(search_term)
        filtered_df = apply_search_filters(filtered_df, parsed_query)

    return filtered_df.sort_values("CODE_COUNT", ascending=False) if "CODE_COUNT" in filtered_df.columns else filtered_df

def display_unified_code_browser(code_types, key_suffix=""):
    """
    Unified code browser that allows selection from global vocabulary or existing definitions
    Args:
        code_types(list):
            List of code types ofr filtering
        key_suffix(str):
            Suffix appended to streamlit widget keys if they are re-used in same page
    """
    st.subheader("Find codes")

    # select source type
    source_type = st.radio(
        "Select code source",
        options=["Global Vocabulary", "Existing Definition"],
        horizontal=True,
        key=f"source_type_radio_{key_suffix}"  # unique key allows re-use in same page (i.e. multiple tabs)
    )

    # search box
    with st.container():
        search_term = st.text_input(
            "Filter codes",
            placeholder="Simple search or use (term1) AND/OR/NOT (term2)",
            help="Examples: 'diabetes', '(heart) AND (failure)', '(cardiac) NOT (surgery)'",
            key=f"search_term_{key_suffix}"
        )

        # filter from different sources
        if source_type == "Global Vocabulary":
            code_type = st.selectbox("Code type",
                                     options=code_types,
                                     label_visibility="collapsed",
                                     key=f"code_type_{key_suffix}")
            filtered_codes = filter_codes(st.session_state.codes, search_term, code_type)
            if not filtered_codes.empty:
                st.write(f"Found {len(filtered_codes):,} codes")
            else:
                st.info("No codes found matching the search criteria")
                return filtered_codes, search_term
        else:
            # conn = connect_to_snowflake()
            id_list, definitions_list = get_definitions_from_snowflake_and_return_as_annotated_list_with_id_list()
            chosen_definition = st.selectbox(
                label="Choose an existing definition (start typing to search)",
                options=definitions_list,
                key=f"chosen_definition_{key_suffix}"
            )

            if chosen_definition:
                chosen_definition_id = id_list[definitions_list.index(chosen_definition)]
                definition_codes_df = return_codes_for_given_definition_id_as_df(chosen_definition_id)

                if search_term:
                    parsed_query = parse_search_query(search_term)
                    filtered_codes = apply_search_filters(definition_codes_df, parsed_query)
                else:
                    filtered_codes = definition_codes_df

                st.write(f"Found {len(filtered_codes):,} codes in definition")
            else:
                st.info("Please select a definition")
                return None, search_term

    # Add all button
    if 'filtered_codes' in locals() and not filtered_codes.empty and st.session_state.current_definition:
        col_button, col_feedback = st.columns([1, 3])
        with col_button:
            if st.button(f"Add all {len(filtered_codes):,} codes", key=f"add_all_{key_suffix}"):
                codes_to_add = []
                for _, row in filtered_codes.iterrows():
                    codes_to_add.append(create_code_from_row(row))

                added, duplicates = st.session_state.current_definition.add_codes_batch(codes_to_add)

                with col_feedback:
                    if added > 0:
                        st.success(f"Added {added} new codes ({duplicates} duplicates skipped)")
                    else:
                        st.info(f"All {duplicates} codes already in definition")
                    st.rerun()

    # results of filter
    with container_object_with_height_if_possible(500):
        if 'filtered_codes' in locals() and not filtered_codes.empty:
            for idx, row in filtered_codes.head(500).iterrows():
                col1a, col1b = st.columns([9, 1])
                with col1a:
                    st.text(f"{row['CODE_DESCRIPTION']} ({row['VOCABULARY']})")

                    # stats only available for global vocabulary
                    if source_type == "Global Vocabulary":
                        basic_info = []
                        if "CODE" in row and pd.notna(row["CODE"]):
                            basic_info.append(f"Code: {row['CODE']}")

                        if "CODE_COUNT" in row and pd.notna(row["CODE_COUNT"]):
                            basic_info.append(f"Count: {row['CODE_COUNT']:,}")

                        if "MEDIAN_AGE" in row and pd.notna(row["MEDIAN_AGE"]):
                            basic_info.append(f"MedianAge: {row['MEDIAN_AGE']:.1f}")

                        if "MEDIAN_VALUE" in row and pd.notna(row["MEDIAN_VALUE"]):
                            basic_info.append(f"MedianValue: {row['MEDIAN_VALUE']:.1f}")

                        if "PERCENT_HAS_RESULT_VALUE" in row and pd.notna(row["PERCENT_HAS_RESULT_VALUE"]):
                            basic_info.append(f"Has Result: {row['PERCENT_HAS_RESULT_VALUE']:.1f}%")

                        if basic_info:
                            st.caption(" | ".join(basic_info))
                    else:
                        # simple view only
                        st.caption(f"Code: {row['CODE']}")

                with col1b:
                    checkbox_key = f"code_{row['CODE']}_{row['VOCABULARY']}"
                    display_code_and_checkbox(row, checkbox_key, key_suffix=key_suffix)
        else:
            st.info("No codes found matching the search criteria")

    return filtered_codes, search_term

def display_selected_codes(key_suffix=""):
    """
    Display the selected codes panel (right panel)
    Args:
        key_suffix(str):
            Suffix appended to streamlit widget keys if they are re-used in same page
    """
    st.subheader("Selected codes")

    # FIXED CONTAINER
    with container_object_with_height_if_possible(210):
        # current definition information
        if st.session_state.current_definition:
            definition = st.session_state.current_definition
            col2c, col2d = st.columns([3, 1])
            with col2c:
                st.write(f"Definition: **{definition.definition_name}**")
            with col2d:
                st.caption(f"ID: {definition.definition_id}")

            # component: save button
            if st.button("Save Definition", key=f"save_def_btn_{key_suffix}"):
                definition.update_version()
                if st.session_state.config["local_development"]:
                    filepath = definition.save_to_json()
                    st.success(f"Definition saved to: {filepath}")
                else:
                    definition.uploaded_datetime = datetime.now()
                    df = definition.to_dataframe()
                    df.columns = df.columns.str.upper()
                    st.session_state.session.write_pandas(df,
                        database=st.session_state.config["definition_library"]["database"],
                        schema=st.session_state.config["definition_library"]["schema"],
                        table_name="ICB_DEFINITIONS",
                        overwrite=False,
                        use_logical_type=True) #  use_logical_type=True is needed to handle datetime columns correctly
                    # - this isn't properly documented anywhere in snowflake docs
                    st.success(f"""Definition saved to Snowflake:
                        {st.session_state.config['definition_library']['database']}.
                        {st.session_state.config['definition_library']['schema']}.ICB_DEFINITIONS""")
        else:
            st.info("Create a definition first.")

    # SCROLLING CONTAINER
    with container_object_with_height_if_possible(1090):
        # if st.session_state.selected_codes:
        if st.session_state.current_definition and st.session_state.current_definition.codes:
            # grouping by vocab (i.e. codelist)
            codes_by_vocab = {}
            # for code in st.session_state.selected_codes:
            for code in st.session_state.current_definition.codes:
                if code.code_vocabulary not in codes_by_vocab:
                    codes_by_vocab[code.code_vocabulary] = []
                codes_by_vocab[code.code_vocabulary].append(code)

            for vocabulary, codes in codes_by_vocab.items():
                st.write(f"**{vocabulary.value}** ({len(codes)} codes)")
                for idx, code in enumerate(codes):
                    col2a, col2b = st.columns([4, 1])
                    with col2a:
                        st.text(f"{code.code_description}")
                        st.caption(f"Code: {code.code}")

                    with col2b:
                        # component: remove button
                        if st.button("Remove", key=f"remove_{vocabulary}_{idx}_{key_suffix}"):
                            # st.session_state.selected_codes.remove(code)
                            if st.session_state.current_definition:
                                st.session_state.current_definition.remove_code(code)
                                print(f"Removed {code.code} from {vocabulary} leaving {st.session_state.current_definition.codes}")
                            st.rerun()

                st.markdown("---")
        elif st.session_state.current_definition:
            st.info("No codes selected. Find and add codes with the search panel.")


def compare_definition_codes(codes_df1, codes_df2):
    """
    Compare two definition code dataframes and return analysis
    """
    codes_1 = set(codes_df1["CODE"])
    codes_2 = set(codes_df2["CODE"])

    shared_codes = codes_1.intersection(codes_2)
    only_in_1 = codes_1.difference(codes_2)
    only_in_2 = codes_2.difference(codes_1)

    return {
        "shared": shared_codes,
        "only_in_1": only_in_1,
        "only_in_2": only_in_2,
        "shared_count": len(shared_codes),
        "only_in_1_count": len(only_in_1),
        "only_in_2_count": len(only_in_2)
    }


def get_missing_codes_df(codes_df, other_codes_df):
    """
    Find codes in other_codes_df that are missing from codes_df
    """
    return other_codes_df[~other_codes_df["CODE"].isin(codes_df["CODE"])]


def display_definition_metadata(codes_df):
    """
    Display definition metadata from a codes dataframe
    """
    if not codes_df.empty:
        st.write("Definition details:")
        st.dataframe(codes_df.loc[:, ["DEFINITION_ID", "CODELIST_VERSION", "VOCABULARY"]].drop_duplicates(),
            hide_index=True,)


def display_definition_codes_summary(codes_df):
    """
    Display codes from a definition dataframe with summary
    """
    if not codes_df.empty:
        st.write("Codes:")
        st.dataframe(codes_df.loc[:, ["CODE", "CODE_DESCRIPTION", "VOCABULARY"]], hide_index=True)
        st.write(f"Total codes: {len(codes_df)}")
    else:
        st.write("No codes found for the selected definition.")

def display_codes_in_selected_definition_simply(codes_df: pd.DataFrame):
    if not codes_df.empty:
            st.markdown(f"""
                <div style="display: flex; align-items: center; gap: 10px;">
                    <span style="
                        background-color: #eee;
                        color: #333;
                        padding: 4px 10px;
                        border-radius: 12px;
                        font-size: 0.85rem;
                        font-weight: 600;
                        display: inline-block;
                    ">
                        {codes_df.loc[0, 'DEFINITION_SOURCE']}
                    </span>
                    <span style="font-weight: 700; font-size: 1rem;">
                        {codes_df.loc[0, 'DEFINITION_NAME']}
                    </span>
                </div>
                """, unsafe_allow_html=True)
            st.write("")

            # Display codes
            st.dataframe(codes_df.loc[:, ["CODE_DESCRIPTION", "VOCABULARY", "CODE"]], hide_index=True)
            st.write(f"Total codes: {len(codes_df)}")

    else:
        st.write("No codes found for the selected definition.")



@st.cache_data(show_spinner=False)
def display_definition_from_file(definition_file):
    """
    Display content from a definition json file
    """
    try:
        file_path = os.path.join("data/definitions", definition_file)
        definition = Definition.from_json(file_path)

        # definition info
        st.caption(f"Definition: {definition.definition_name}")

        # codelists and codes
        total_codes = 0
        for codelist in definition.codelists:
            with st.expander(f"{codelist.codelist_vocabulary.value} ({len(codelist.codes)} codes)"):
                for code in codelist.codes:
                    st.text(f"{code.code}: {code.code_description}")
                total_codes += len(codelist.codes)

        st.info(f"Total: {len(definition.codelists)} codelists, {total_codes} codes")

        return definition
    except Exception as e:
        st.error(f"Error loading definition: {e}")
        return None

def process_definitions_for_upload(
        definition_files: List[str],
        config: Optional[dict] = None,
        session: Optional[Session] = None) -> Tuple[pd.DataFrame, List[str], dict]:
    """
    Process all definition files and prepare them for upload to Snowflake

    Args:
        definition_files (List[str]):
            List of definition file names to process
        config (Optional[dict]):
            Configuration dictionary containing database and schema information. If not provided,
            will use the session state from Streamlit. Leave as None if calling from within Streamlit app.
        session (Optional[Session]):
            Snowflake session to use for database operations. If not provided, will use the session state from Streamlit.
            Leave as None if calling from within Streamlit app.
    """
    config = config or st.session_state.config
    session = session or st.session_state.session

    if not definition_files:
        return None, [], {}

    all_rows = pd.DataFrame()
    definitions_to_remove = {}
    definitions_to_add = []

    for def_file in definition_files:
        file_path = os.path.join("data/definitions", def_file)
        definition = Definition.from_json(file_path)

        query = f"""
        SELECT DEFINITION_ID, DEFINITION_NAME, VERSION_DATETIME
        FROM {config["definition_library"]["database"]}.
        {config["definition_library"]["schema"]}.
        AIC_DEFINITIONS
        WHERE DEFINITION_ID = '{definition.definition_id}'
        """
        existing_definition = session.sql(query).to_pandas()

        if not existing_definition.empty:
            max_version_in_db = existing_definition["VERSION_DATETIME"].max()
            current_version = definition.version_datetime

            if current_version == max_version_in_db:
                continue  # skip if already exists

            if current_version < max_version_in_db:
                continue  # skip if newer version exists

            # record that we want to delete the old one
            definitions_to_remove[definition.definition_id] = [definition.definition_name, current_version]

        definition.uploaded_datetime = datetime.now()

        all_rows = pd.concat([all_rows, definition.to_dataframe()])
        definitions_to_add.append(definition.definition_name)

    return all_rows, definitions_to_add, definitions_to_remove

def update_aic_definitions_table(config: Optional[dict] = None, session: Optional[Session] = None):
    """
    Overwrite the AIC_DEFINITIONS table with definitions from local files.

    Args:
        config (Optional[dict]):
            Configuration dictionary containing database and schema information. If not provided,
            will use the session state from Streamlit. Leave as None if calling from within Streamlit app.
        session (Optional[Session]):
            Snowflake session to use for database operations. If not provided, will use the session state from Streamlit.
            Leave as None if calling from within Streamlit app.
    """
    config = config or st.session_state.config
    session = session or st.session_state.session

    # Get definition files
    definition_files = load_definitions_list_from_local_files()

    # Process all definition files
    all_rows = pd.DataFrame()

    for def_file in definition_files:
        file_path = os.path.join("data/definitions", def_file)
        definition = Definition.from_json(file_path)
        definition.uploaded_datetime = datetime.now()
        all_rows = pd.concat([all_rows, definition.to_dataframe()])

    if not all_rows.empty:
        df = all_rows.copy()
        df.columns = df.columns.str.upper()
        session.write_pandas(df,
                database=config["definition_library"]["database"],
                schema=config["definition_library"]["schema"],
                table_name="AIC_DEFINITIONS",
                overwrite=True,
                use_logical_type=True)
        print(f"Uploaded AIC_DEFINITIONS table with {len(all_rows)} rows")
    else:
        print("No definitions found to load")
