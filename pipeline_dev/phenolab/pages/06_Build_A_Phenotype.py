import os
import sys

import streamlit as st
from dotenv import load_dotenv

from utils.phenotype import ComparisonOperator, ConditionType, Phenotype, load_phenotype_from_json

from phmlondon.snow_utils import SnowflakeConnection

load_dotenv()

def load_phenotypes_list():
    """
    Get list of phenotype files from /data/phenotypes
    """
    phenotypes_list = []
    try:
        if os.path.exists("data/phenotypes"):
            phenotypes_list = [f for f in os.listdir("data/phenotypes") if f.endswith(".json")]
    except Exception as e:
        st.error(f"Unable to list phenotype files: {e}")

    return phenotypes_list

def load_phenotype(file_path):
    """
    Load phenotype from json file
    """
    try:
        phenotype = load_phenotype_from_json(file_path)
        return phenotype
    except Exception as e:
        st.error(f"Unable to load phenotype: {e}")
        return None

def query_definition_store(snowsesh, search_term, source_system):
    """
    Query the DEFINITIONSTORE view with filters
    """
    query = """
    SELECT DISTINCT
        DEFINITION_ID,
        DEFINITION_NAME,
        DEFINITION_VERSION,
        DEFINITION_SOURCE,
        SOURCE_LOADER,
        COUNT(*) AS CODE_COUNT
    FROM INTELLIGENCE_DEV.AI_CENTRE_DEFINITION_LIBRARY.DEFINITIONSTORE
    """

    where_clauses = []

    if source_system != "All":
        where_clauses.append(f"SOURCE_LOADER = '{source_system}'")

    if search_term:
        where_clauses.append(f"DEFINITION_NAME ILIKE '%{search_term}%'")

    if where_clauses:
        query += " WHERE " + " AND ".join(where_clauses)

    query += """
    GROUP BY DEFINITION_ID, DEFINITION_NAME, DEFINITION_VERSION, DEFINITION_SOURCE, SOURCE_LOADER
    ORDER BY DEFINITION_NAME
    LIMIT 100
    """

    try:
        return snowsesh.execute_query_to_df(query)
    except Exception as e:
        st.error(f"Error querying DEFINITIONSTORE: {e}")
        return None

def display_panel_1_phenotype_management():
    """
    Panel 1: Phenotype creation and selection
    """
    st.subheader("1. Phenotype Management")

    with st.expander("Creating Phenotypes", expanded=False):
        st.write("""
        **Creating or Loading a Phenotype**
        - To create a new phenotype, enter a name and description, then click "Create new phenotype"
        - To edit an existing phenotype, select from the dropdown and click "Edit phenotype"
        """)

    col1, col2, col3 = st.columns([2, 1, 1])

    # load existing
    with col1:
        phenotypes_list = load_phenotypes_list()
        selected_phenotype_file = st.selectbox(
            "Select existing phenotype",
            options=phenotypes_list,
            index=0 if phenotypes_list else None,
            key="phenotype_selector"
        )

        if selected_phenotype_file and st.button("Edit phenotype"):
            with st.spinner("Loading phenotype..."):
                file_path = os.path.join("data/phenotypes", selected_phenotype_file)
                phenotype = load_phenotype(file_path)
                if phenotype:
                    st.session_state.current_phenotype = phenotype
                    st.success(f"Loaded phenotype: {phenotype.phenotype_name}")
                    st.rerun()

    # create new
    with col2:
        new_phenotype_name = st.text_input("New phenotype name")

    with col3:
        new_phenotype_desc = st.text_input("Description")
        if new_phenotype_name and st.button("Create new phenotype"):
            st.session_state.current_phenotype = Phenotype(
                phenotype_name=new_phenotype_name,
                description=new_phenotype_desc
            )
            st.success(f"Created new phenotype: {new_phenotype_name}")
            st.rerun()

def display_panel_2_definition_selection():
    """
    Panel 2: Definition search and selection
    """
    st.subheader("2. Definition Selection")

    with st.expander("Selecting Definitions", expanded=False):
        st.write("""
        **Searching for Definitions**
        - Select a source system from DEFINITIONSTORE using the dropdown
        - Enter search terms to filter definitions by name
        - Click "Search" to find matching definitions
        - Click "Add" on a definition to select it for your phenotype
        """)

    if "snowsesh" not in st.session_state:
        with st.spinner("Connecting to Snowflake..."):
            try:
                st.session_state.snowsesh = SnowflakeConnection()
                st.session_state.snowsesh.use_database("INTELLIGENCE_DEV")
                st.session_state.snowsesh.use_schema("AI_CENTRE_DEFINITION_LIBRARY")
            except Exception as e:
                st.error(f"Failed to connect to Snowflake: {e}")
                return

    # get available sources
    if "source_systems" not in st.session_state:
        try:
            source_query = """
            SELECT DISTINCT SOURCE_LOADER FROM INTELLIGENCE_DEV.AI_CENTRE_DEFINITION_LIBRARY.DEFINITIONSTORE
            WHERE SOURCE_LOADER IS NOT NULL
            ORDER BY SOURCE_LOADER
            """
            sources_df = st.session_state.snowsesh.execute_query_to_df(source_query)
            source_systems = ["All"] + sources_df["SOURCE_LOADER"].tolist()
            st.session_state.source_systems = source_systems
        except Exception as e:
            st.error(f"Error fetching source systems: {e}")
            st.session_state.source_systems = ["All"]

    # filter controls
    source_system = st.selectbox(
        "Source system",
        options=st.session_state.source_systems
    )

    search_term = st.text_input("Search definitions")

    if st.button("Search"):
        with st.spinner("Searching definitions..."):
            results = query_definition_store(st.session_state.snowsesh, search_term, source_system)
            if results is not None and not results.empty:
                st.session_state.definition_results = results
                st.success(f"Found {len(results)} definitions")
            else:
                st.session_state.definition_results = None
                st.warning("No definitions found")

    # display results
    if "definition_results" in st.session_state and st.session_state.definition_results is not None:
        with st.container(height=300):
            st.write("Definitions:")
            for idx, row in st.session_state.definition_results.iterrows():
                col1, col2 = st.columns([3, 1])

                with col1:
                    st.write(f"**{row['DEFINITION_NAME']}**")
                    st.caption(f"ID: {row['DEFINITION_ID']} | Source: {row['DEFINITION_SOURCE']} | Codes: {row['CODE_COUNT']}")

                with col2:
                    # form to configure condition
                    if st.button("Add", key=f"add_{idx}"):
                        st.session_state.selected_definition = {
                            "id": row["DEFINITION_ID"],
                            "name": row["DEFINITION_NAME"],
                            "source": row["DEFINITION_SOURCE"]
                        }
                        st.rerun()

def display_panel_3_condition_configuration():
    """
    Panel 3: Configure selected definition as a condition
    """
    st.subheader("3. Condition Configuration")

    with st.expander("Configuring Conditions", expanded=False):
        st.write("""
        **Configuring Conditions**
        - Choose the condition type:
        - HAS: Patient has a code from this definition
        - MEASURE: Measurement from this definition meets specified criteria
        - For MEASURE type, set comparison operator, threshold value, and unit
        - Click "Add to phenotype" to add this as a condition block
        """)

    if not st.session_state.current_phenotype:
        st.info("Please create or load a phenotype first")
        return

    # configure definition
    if "selected_definition" in st.session_state:
        st.write(f"Configuring condition for: **{st.session_state.selected_definition['name']}**")

        with st.form(key="condition_form"):
            condition_type = st.radio(
                "Condition type",
                options=[ConditionType.HAS_DEFINITION.value, ConditionType.MEASUREMENT.value]
            )

            # always show measurement parameters
            # **couldn't get conditional display to work**
            st.markdown("### Measurement Parameters (only used for MEASURE type)")

            comparison_operator = st.selectbox(
                "Comparison operator",
                options=[op.value for op in ComparisonOperator]
            )

            col1, col2 = st.columns([2, 1])
            with col1:
                threshold_value = st.number_input(
                    "Threshold value",
                    value=0.0,
                    step=0.1
                )
            with col2:
                threshold_unit = st.text_input(
                    "Unit (e.g., mmHg)",
                    value=""
                )

            submit_button = st.form_submit_button("Add to phenotype")

            if submit_button:
                # for HAS_DEFINITION, ignore the measurement parameters
                if condition_type == ConditionType.HAS_DEFINITION.value:
                    comparison_operator = None
                    threshold_value = None
                    threshold_unit = None

                definition_source = st.session_state.selected_definition.get("source", "CUSTOM")

                # add the condition block to the phenotype
                st.session_state.current_phenotype.add_condition_block(
                    definition_id=st.session_state.selected_definition["id"],
                    definition_name=st.session_state.selected_definition["name"],
                    definition_source=definition_source,
                    condition_type=condition_type,
                    comparison_operator=comparison_operator,
                    threshold_value=threshold_value,
                    threshold_unit=threshold_unit
                )

                # clear deftiniion at end
                del st.session_state.selected_definition
                st.rerun()
    else:
        st.info("Select a Definition to configure it as a condition")

def display_panel_4_condition_blocks():
    """
    Panel 4: Display and manage added condition blocks
    """
    st.subheader("4. Condition Blocks")

    with st.expander("Manage Selected Blocks", expanded=False):
        st.write("""
        **Managing Condition Blocks**
        - This panel shows all condition blocks you've added
        - Each block is assigned a letter (A, B, C, etc.)
        - You can remove any block by clicking the "Remove" button
        - Note these block labels for use in the logical expression builder
        """)
    if not st.session_state.current_phenotype:
        st.info("Please create or load a phenotype first")
        return

    phenotype = st.session_state.current_phenotype

    if not phenotype.condition_blocks:
        st.info("No condition blocks added yet.")
        return

    # Display each condition block with details and Remove button
    with st.container(height=300):
        for label, block in phenotype.condition_blocks.items():
            with st.container():
                col1, col2 = st.columns([4, 1])

                with col1:
                    st.write(f"**Block {label}**: {block.to_dsl_description()}")
                    st.caption(f"Definition ID: {block.definition_id}")

                with col2:
                    if st.button("Remove", key=f"remove_{label}"):
                        phenotype.remove_condition_block(label)
                        st.rerun()

                st.markdown("---")

def display_panel_5_expression_builder():
    """
    Panel 5: Build logical expressions
    """
    st.subheader("5. Logic Expression Builder")

    with st.expander("Building Logical Expressions", expanded=False):
        st.write("""
        **Building Logical Expressions**
        - Use the block labels (A, B, C, etc.) to build your expression
        - Use operators: AND, OR, NOT
        - Use parentheses to group expressions: (A AND B)
        - Example: ((A AND B) OR C) NOT D
        - Click "Validate Expression" to check your syntax
        - Click "Save Phenotype" when finished
        """)

    if not st.session_state.current_phenotype:
        st.info("Please create or load a Phenotype first")
        return

    phenotype = st.session_state.current_phenotype

    if not phenotype.condition_blocks:
        st.info("Add condition blocks to Phenotype first")
        return

    # Helper text
    st.write("Available blocks:")
    for label, block in phenotype.condition_blocks.items():
        st.write(f"**{label}**: {block.to_dsl_description()}")

    st.write("Build your expression using blocks, operators (AND, OR, NOT), and parentheses.")
    st.write("Example: (A AND B) OR C")

    # Expression input
    expression = st.text_input(
        "Enter logical expression",
        value=phenotype.expression,
    )

    col1, col2 = st.columns([1, 1])

    with col1:
        if st.button("Validate Expression"):
            if expression:
                phenotype.update_expression(expression)
                valid, message = phenotype.validate_expression()

                if valid:
                    st.success("Expression is valid")
                    st.write("Expanded expression:")
                    st.write(phenotype.get_expanded_expression())
                else:
                    st.error(f"Invalid expression: {message}")

    with col2:
        # Save phenotype
        if st.button("Save Phenotype"):
            if not phenotype.expression:
                st.error("Cannot save: Expression is empty")
                return

            valid, message = phenotype.validate_expression()
            if valid:
                try:
                    filepath = phenotype.save_to_json()
                    st.success(f"Phenotype saved to: {filepath}")
                except Exception as e:
                    st.error(f"Error saving phenotype: {e}")
            else:
                st.error(f"Cannot save: {message}")

def main():
    st.set_page_config(page_title="Build A Phenotype", layout="wide")
    st.title("Build a phenotype")

    if "current_phenotype" not in st.session_state:
        st.session_state.current_phenotype = None

    # Panel 1: Phenotype Management
    display_panel_1_phenotype_management()
    st.markdown("---")

    # Panel 2 & 3: Definition Selection and Condition Configuration
    col1, col2 = st.columns([1, 1])
    with col1:
        display_panel_2_definition_selection()

    with col2:
        display_panel_3_condition_configuration()

    st.markdown("---")

    # Panel 4: Condition Blocks Display
    display_panel_4_condition_blocks()
    st.markdown("---")

    # Panel 5: Expression Builder
    display_panel_5_expression_builder()

if __name__ == "__main__":
    main()
