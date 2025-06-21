import os

import streamlit as st
from dotenv import load_dotenv
from utils.database_utils import get_snowflake_session, get_available_measurements
from utils.phenotype import ComparisonOperator, ConditionType, Phenotype, load_phenotype_from_json
from utils.style_utils import set_font_lato, container_object_with_height_if_possible   
from utils.config_utils import load_config

# # 05_Build_A_Phenotype.py

# This page facilitates the creation of phenotypes by combining clinical definitions /
# with logical operators. Users can select definitions, configure condition blocks, /
# and build logical expressions to define cohorts.

# TO DO
# - Not feature complete!


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


def query_definition_store(session, search_term, source_system):
    """
    Query the DEFINITIONSTORE view with filters
    """
    query = f"""
    SELECT DISTINCT
        DEFINITION_ID,
        DEFINITION_NAME,
        DEFINITION_VERSION,
        DEFINITION_SOURCE,
        SOURCE_TABLE,
        COUNT(*) AS CODE_COUNT
    FROM {st.session_state.config["definition_library"]["database"]}.
    {st.session_state.config["definition_library"]["schema"]}.DEFINITIONSTORE
    """

    where_clauses = []

    if source_system != "All":
        where_clauses.append(f"SOURCE_TABLE = '{source_system}'")

    if search_term:
        where_clauses.append(f"DEFINITION_NAME ILIKE '%{search_term}%'")

    if where_clauses:
        query += " WHERE " + " AND ".join(where_clauses)

    query += """
    GROUP BY DEFINITION_ID, DEFINITION_NAME, DEFINITION_VERSION, DEFINITION_SOURCE, SOURCE_TABLE
    ORDER BY DEFINITION_NAME
    LIMIT 100
    """

    try:
        # return snowsesh.execute_query_to_df(query)
        return session.sql(query).to_pandas()
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
            key="phenotype_selector",
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
                phenotype_name=new_phenotype_name, description=new_phenotype_desc
            )
            st.success(f"Created new phenotype: {new_phenotype_name}")
            st.rerun()


def display_panel_2_definition_selection():
    """
    Panel 2: Definition search and selection for HAS conditions
    """
    st.subheader("2. Add HAS Condition")

    with st.expander("Creating HAS Conditions", expanded=False):
        st.write("""
        **HAS Conditions**
        - Search for clinical definitions (code lists) from the definition store
        - Click "Add HAS Condition" to create a condition that checks if a patient has any code from the selected definition
        - Example: "Patient has any diabetes diagnosis code"
        """)

    # Get the single connection (already connected to definition library by default)
    try:
        # snowsesh = get_snowflake_connection()
        session = get_snowflake_session()
    except Exception as e:
        st.error(f"Failed to get Snowflake connection: {e}")
        return

    # get available sources
    if "source_systems" not in st.session_state:
        try:
            source_query = f"""
            SELECT DISTINCT SOURCE_TABLE FROM {st.session_state.config["definition_library"]["database"]}.
            {st.session_state.config["definition_library"]["schema"]}.DEFINITIONSTORE
            WHERE SOURCE_TABLE IS NOT NULL
            ORDER BY SOURCE_TABLE
            """
            # sources_df = snowsesh.execute_query_to_df(source_query)
            sources_df = session.sql(source_query).to_pandas()
            source_systems = ["All"] + sources_df["SOURCE_TABLE"].tolist()
            st.session_state.source_systems = source_systems
        except Exception as e:
            st.error(f"Error fetching source systems: {e}")
            st.session_state.source_systems = ["All"]

    # filter controls
    source_system = st.selectbox("Source system", options=st.session_state.source_systems)

    search_term = st.text_input("Search definitions")

    if st.button("Search"):
        with st.spinner("Searching definitions..."):
            results = query_definition_store(session, search_term, source_system)
            if results is not None and not results.empty:
                st.session_state.definition_results = results
                st.success(f"Found {len(results)} definitions")
            else:
                st.session_state.definition_results = None
                st.warning("No definitions found")

    # display results
    if "definition_results" in st.session_state and st.session_state.definition_results is not None:
        with container_object_with_height_if_possible(height=300):
            st.write("Definitions:")
            for idx, row in st.session_state.definition_results.iterrows():
                col1, col2 = st.columns([3, 1])

                with col1:
                    st.write(f"**{row['DEFINITION_NAME']}**")
                    st.caption(
                        f"ID: {row['DEFINITION_ID']} | Source: {row['DEFINITION_SOURCE']} | Codes: {row['CODE_COUNT']}"
                    )

                with col2:
                    if st.button("Add HAS Condition", key=f"add_has_{idx}"):
                        if not st.session_state.current_phenotype:
                            st.error("Please create or load a phenotype first")
                        else:
                            # Directly add HAS condition
                            st.session_state.current_phenotype.add_condition_block(
                                definition_id=row["DEFINITION_ID"],
                                definition_name=row["DEFINITION_NAME"],
                                definition_source=row["DEFINITION_SOURCE"],
                                condition_type=ConditionType.HAS_DEFINITION,
                            )
                            st.success(f"Added HAS condition for {row['DEFINITION_NAME']}")
                            st.rerun()


def display_panel_3_measurement_selection():
    """
    Panel 3: Measurement feature store selection for MEASUREMENT conditions
    """
    st.subheader("3. Add MEASUREMENT Condition")

    with st.expander("Creating MEASUREMENT Conditions", expanded=False):
        st.write("""
        **MEASUREMENT Conditions**
        - Browse measurement features from the feature store
        - Configure thresholds, temporal patterns, and data quality filters
        - Example: "2 BP systolic measurements >140 mmHg within 14 days"
        """)

    if not st.session_state.current_phenotype:
        st.info("Please create or load a phenotype first")
        return

    try:
        # snowsesh = get_snowflake_connection()
        session = get_snowflake_session()
    except Exception as e:
        st.error(f"Failed to get Snowflake connection: {e}")
        return

    # measurement_features = get_available_measurements(snowsesh)
    measurement_features = get_available_measurements(session, st.session_state.config)

    if measurement_features.empty:
        st.warning("No measurement features found in the feature store")
        return

    search_term = st.text_input("Search measurement features")

    if search_term:
        filtered_features = measurement_features[
            measurement_features['DEFINITION_NAME'].str.contains(search_term, case=False, na=False)
        ]
    else:
        filtered_features = measurement_features

    st.write(f"Found {len(filtered_features)} measurement features")

    with container_object_with_height_if_possible(height=300):
        for idx, row in filtered_features.iterrows():
            col1, col2 = st.columns([3, 1])

            with col1:
                st.write(f"**{row['DEFINITION_NAME']}**")
                st.caption(f"Units: {row['VALUE_UNITS']} | Count: {row['MEASUREMENT_COUNT']:,} measurements")
                st.caption(f"ID: {row['DEFINITION_ID']} | Table: {row['TABLE_NAME']}")

            with col2:
                if st.button("Configure", key=f"config_measure_{idx}"):
                    st.session_state.selected_measurement = {
                        "id": row["DEFINITION_ID"],
                        "name": row["DEFINITION_NAME"],
                        "units": row["VALUE_UNITS"],
                        "table_name": row["TABLE_NAME"],
                    }
                    st.rerun()

    # configure selected measurement
    if "selected_measurement" in st.session_state:
        st.markdown("---")
        st.write(f"### Configuring MEASUREMENT condition for: **{st.session_state.selected_measurement['name']}**")

        with st.form(key="measurement_form"):

            st.markdown("#### Threshold Configuration")
            comparison_operator = st.selectbox("Comparison operator", options=[op.value for op in ComparisonOperator])

            col1, col2 = st.columns([2, 1])
            with col1:
                threshold_value = st.number_input("Threshold value", value=0.0, step=0.1)
            with col2:
                threshold_unit = st.text_input("Unit (e.g., mmHg)", value=st.session_state.selected_measurement.get("units", ""))

            st.markdown("#### Temporal Pattern")
            col3, col4 = st.columns([1, 1])
            with col3:
                number_of_measures = st.number_input("Number of measures required", min_value=1, value=1,
                                                   help="How many measurements must meet the threshold")
            with col4:
                measure_time_window_days = st.number_input("Time window (days)", min_value=1, value=None,
                                                         help="Time window for collecting measurements (leave blank for any time)")

            st.markdown("#### Data Quality Filters")
            col5, col6 = st.columns([1, 1])
            with col5:
                value_lower_cutoff = st.number_input("Lower cutoff (exclude values below)", value=None,
                                                   help="Exclude measurements below this value for data quality")
            with col6:
                value_upper_cutoff = st.number_input("Upper cutoff (exclude values above)", value=None,
                                                   help="Exclude measurements above this value for data quality")

            submit_button = st.form_submit_button("Add MEASUREMENT Condition")

            if submit_button:
                st.session_state.current_phenotype.add_condition_block(
                    definition_id=st.session_state.selected_measurement["id"],
                    definition_name=st.session_state.selected_measurement["name"],
                    definition_source="FEATURE_STORE",
                    condition_type=ConditionType.MEASUREMENT,
                    comparison_operator=comparison_operator,
                    threshold_value=threshold_value,
                    threshold_unit=threshold_unit,
                    number_of_measures=number_of_measures,
                    measure_time_window_days=measure_time_window_days,
                    value_lower_cutoff=value_lower_cutoff,
                    value_upper_cutoff=value_upper_cutoff,
                )

                st.success(f"Added MEASUREMENT condition for {st.session_state.selected_measurement['name']}")
                del st.session_state.selected_measurement
                st.rerun()


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

    with container_object_with_height_if_possible(300):
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

    st.write("Available blocks:")
    for label, block in phenotype.condition_blocks.items():
        st.write(f"**{label}**: {block.to_dsl_description()}")

    st.write("Build your expression using blocks, operators (AND, OR, NOT), and parentheses.")
    st.write("Example: (A AND B) OR C")

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
    set_font_lato()
    if "config" not in st.session_state:
        st.session_state.config = load_config(get_snowflake_session())
    st.title("Build a phenotype")
    # load_dotenv()

    if "current_phenotype" not in st.session_state:
        st.session_state.current_phenotype = None

    # Panel 1: Phenotype Management
    display_panel_1_phenotype_management()
    st.markdown("---")

    # Panel 2 & 3: HAS and MEASUREMENT Condition Creation
    col1, col2 = st.columns([1, 1])
    with col1:
        display_panel_2_definition_selection()

    with col2:
        display_panel_3_measurement_selection()

    st.markdown("---")

    # Panel 4: Condition Blocks Display
    display_panel_4_condition_blocks()
    st.markdown("---")

    # Panel 5: Expression Builder
    display_panel_5_expression_builder()


if __name__ == "__main__":
    main()
