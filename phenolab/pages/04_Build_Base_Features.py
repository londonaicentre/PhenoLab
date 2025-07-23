import plotly.graph_objects as go
import streamlit as st
from plotly.subplots import make_subplots

from utils.condition_interaction_utils import (
    create_base_conditions_feature,
    get_non_measurement_definitions,
)
from utils.database_utils import (
    get_condition_patient_counts_by_year,
    get_snowflake_session,
    get_unique_patients_for_condition,
)
from utils.measurement_interaction_utils import (
    apply_conversions,
    apply_unit_mapping,
    create_base_measurements_feature,
    get_available_measurement_configs,
    get_measurement_values,
)
from utils.style_utils import set_font_lato
from utils.config_utils import load_config

# # 05_Base_Features.py

# Page for viewing definition and measurement distributions
# Creating base feature stores for definition flags and measurements.



def create_distribution_plots(df_all, config):
    """
    Create primary unit and percentiles disdtribution pluts
    """
    # apply 99.5 percentile cutoff - otherwise extreme outliers will hide true distribution
    percentile_995 = df_all['value'].quantile(0.995) if not df_all.empty else float('inf')
    df_all_filtered = df_all[df_all['value'] <= percentile_995].copy()

    if 'converted_value' in df_all_filtered.columns:
        converted_percentile_995 = df_all_filtered['converted_value'].quantile(0.995) \
            if not df_all_filtered.empty else float('inf')
        df_all_filtered = df_all_filtered[df_all_filtered['converted_value'] <= converted_percentile_995]

    df_primary = df_all_filtered[['converted_value', 'converted_unit']].rename(columns={'converted_value': 'value',
                                                                                        'converted_unit': 'unit'})

    # Create 1x2 subplot grid
    fig = make_subplots(
        rows=1, cols=2,
        subplot_titles=(
            f"Primary Unit: {config.primary_standard_unit} ({len(df_primary):,} values)",
            f"Percentile Distribution ({config.primary_standard_unit})"
        ),
        horizontal_spacing=0.15,
        specs=[[{"type": "histogram"}, {"type": "scatter"}]]
    )

    # plot 1: primary (left)
    if not df_primary.empty and config.primary_standard_unit:
        fig.add_trace(
            go.Histogram(x=df_primary['value'], name=config.primary_standard_unit, nbinsx=50, marker_color='darkgreen'),
            row=1, col=1
        )

    # plot 2: percentiles (right)
    if not df_all.empty and 'converted_value' in df_all.columns:
        converted_values = df_all['converted_value'].dropna()
        if not converted_values.empty:
            percentiles = [0.005, 0.01, 0.05, 0.10, 0.25, 0.50, 0.75, 0.90, 0.95, 0.99, 0.995]
            percentile_values = [converted_values.quantile(p) for p in percentiles]
            percentile_labels = [f"{p*100:.1f}%" for p in percentiles]

            fig.add_trace(go.Scatter(
                x=percentile_labels,
                y=percentile_values,
                mode='lines+markers',
                marker=dict(size=8, color='darkgreen'),
                line=dict(color='darkgreen', width=2),
                name='Percentiles'
            ), row=1, col=2)

            # draw Median
            median_idx = percentiles.index(0.50)
            fig.add_hline(
                y=percentile_values[median_idx],
                line_dash="dash",
                line_color="gray",
                annotation_text=f"Median: {percentile_values[median_idx]:.2f}",
                row=1, col=2
            )

    fig.update_xaxes(title_text="Value", row=1, col=1)
    fig.update_xaxes(title_text="Percentile", row=1, col=2)

    fig.update_yaxes(title_text="Count", row=1, col=1)
    fig.update_yaxes(title_text=f"Value ({config.primary_standard_unit})", row=1, col=2)

    fig.update_layout(
        height=400,
        showlegend=False,
        title_text=f"Measurement Value Distributions: {config.definition_name} (99.5 percentile cutoff applied)"
    )

    return fig



def display_measurement_analysis():
    config_options = get_available_measurement_configs()

    if not config_options:
        st.warning("No measurement configurations with standard units defined. " \
        "Please define standard units in the Measurement Standardisation page first.")
        return

    selected_measurement = st.selectbox(
        "Select a measurement to view distributions",
        options=sorted(config_options.keys()),
        key="measurement_distribution_select"
    )

    if selected_measurement:
        config = config_options[selected_measurement]

        if not config.primary_standard_unit:
            st.warning(f"No primary unit set for {selected_measurement}." \
                       "Please set a primary unit in the Measurement Standardisation page.")
            return

        with st.spinner("Loading measurement values..."):
            df_values = get_measurement_values(selected_measurement)

        if df_values.empty:
            st.warning(f"No measurement values found for {selected_measurement}")
            return

        df_mapped = apply_unit_mapping(df_values, config)
        unmapped_count = df_mapped['mapped_unit'].isna().sum()

        st.info(f"Loaded {len(df_values):,} measurement values (Unmapped = {unmapped_count:,})")

        df_all = apply_conversions(df_mapped, config)

        fig = create_distribution_plots(df_all, config)
        st.plotly_chart(fig, use_container_width=True)


def display_feature_creation():
    eligible_configs = get_available_measurement_configs()

    if not eligible_configs:
        st.warning("No measurement configurations found. " \
        "Please ensure measurements have standard units defined, " \
        "primary standard unit is set, and unit mappings are defined.")
        return

    st.write("""This will create or update the **Base Measurements** feature table \
             containing converted values from the standardised measurements shown on this page.\
             """)

    if st.button("Create / Update Base Measurements Table", type="primary", use_container_width=True):
        create_base_measurements_feature(eligible_configs)

    st.write("""
    **Table Schema:**
    - `PERSON_ID`: Patient identifier
    - `CLINICAL_EFFECTIVE_DATE`: Date of measurement
    - `AGE_AT_EVENT`: Age of patient at time of measurement
    - `DEFINITION_ID`: Measurement definition ID
    - `DEFINITION_NAME`: Measurement definition name
    - `SOURCE_RESULT_VALUE`: Original measurement value
    - `SOURCE_RESULT_VALUE_UNITS`: Original measurement unit
    - `VALUE_AS_NUMBER`: Converted value in primary standard unit
    - `VALUE_UNITS`: Primary standard unit (standardized)
    """)

def create_condition_distribution_plot(df_yearly, definition_name):
    """
    Create a bar chart showing patient counts by year
    """
    fig = go.Figure()

    fig.add_trace(go.Bar(
        x=df_yearly['YEAR'],
        y=df_yearly['PATIENT_COUNT'],
        marker_color='darkblue',
        name='Patient Count'
    ))

    fig.update_layout(
        title=f"Unique Patients by Year: {definition_name}",
        xaxis_title="Year",
        yaxis_title="Number of Unique Patients",
        height=400,
        showlegend=False
    )

    return fig


def display_condition_analysis():
    """
    Display condition analysis with patient counts by year
    Returns the selected definition source for use in other components
    """
    # dropdown to select definition source
    definition_source = st.selectbox(
        "Select definition source",
        options=["AIC", "ICB"],
        format_func=lambda x: "AIC Definitions (AI Centre)" if x == "AIC" else "ICB Definitions (User Created)",
        key="definition_source_select"
    )

    definitions = get_non_measurement_definitions(source=definition_source)

    if not definitions:
        source_description = "AI Centre definitions in data/definitions folder" if definition_source == "AIC" else "ICB definitions in Snowflake"
        st.warning(f"No non-measurement definitions found. Please ensure {source_description} exist.")
        return

    selected_condition = st.selectbox(
        "Select a condition to view patient counts",
        options=sorted(definitions.keys()),
        key="condition_distribution_select"
    )

    if selected_condition:
        with st.spinner("Loading patient counts..."):
            df_yearly = get_condition_patient_counts_by_year(selected_condition)

        if df_yearly.empty:
            st.warning(f"No patients found for {selected_condition}")
            return

        total_observations = df_yearly['PATIENT_COUNT'].sum()
        unique_patients = get_unique_patients_for_condition(selected_condition)

        st.info(f"Total unique patients: {unique_patients:,} (Total observations: {total_observations:,})")

        fig = create_condition_distribution_plot(df_yearly, selected_condition)
        st.plotly_chart(fig, use_container_width=True)

    return definition_source


def display_condition_feature_creation(definition_source):
    """
    Display UI for creating Base Conditions feature table

    Args:
        definition_source:
            "AIC" or "ICB" to determine which definitions to use
    """
    definitions = get_non_measurement_definitions(source=definition_source)

    if not definitions:
        source_description = "AI Centre definitions" if definition_source == "AIC" else "ICB definitions"
        st.warning(f"No non-measurement {source_description} found. Please add definitions first.")
        return

    # Update table name and description based on source
    table_name = "Base ICB Conditions" if definition_source == "ICB" else "Base Conditions"
    table_suffix = "(ICB)" if definition_source == "ICB" else "(AIC)"

    st.write(f"""This will create or update the **{table_name}** feature table
             containing condition flags for ALL {len(definitions)} non-measurement definitions {table_suffix}.
             """)

    button_text = f"Create / Update {table_name} Table"
    if st.button(button_text, type="primary", use_container_width=True):
        all_definitions = list(definitions.keys())
        create_base_conditions_feature(all_definitions, source=definition_source)

    st.write("""
    **Table Schema:**
    - `PERSON_ID`: Patient identifier
    - `CLINICAL_EFFECTIVE_DATE`: Date of condition observation (or ACTIVITY_DATE for ICD10/OPCS4)
    - `DEFINITION_ID`: Condition definition ID
    - `DEFINITION_NAME`: Condition definition name
    - `SOURCE_VOCABULARY`: Source vocabulary (SNOMED, ICD10, or OPCS4)
    """)


def main():
    st.set_page_config(page_title="Base Feature Creation", layout="wide")
    set_font_lato()
    if "session" not in st.session_state:
        st.session_state.session = get_snowflake_session()
    if "config" not in st.session_state:
        st.session_state.config = load_config()
    st.title("Distributions & Base Feature Creation")
    # load_dotenv()

    # snowsesh = get_snowflake_connection()

    tab1, tab2 = st.tabs(["Has Condition", "Measurements"])

    with tab1:
        left_col, right_col = st.columns([2, 1])

        with left_col:
            display_condition_analysis()

        with right_col:
            # Get the definition source from session state (set by the dropdown in left column)
            definition_source = st.session_state.get("definition_source_select", "AIC")
            display_condition_feature_creation(definition_source)

    with tab2:
        left_col, right_col = st.columns([2, 1])

        with left_col:
            display_measurement_analysis()

        with right_col:
            display_feature_creation()


if __name__ == "__main__":
    main()