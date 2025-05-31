import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from dotenv import load_dotenv
from plotly.subplots import make_subplots
from utils.database_utils import get_snowflake_connection
from utils.measurement_interaction_utils import (
    apply_conversions,
    apply_unit_mapping,
    create_base_measurements_feature,
    get_available_measurement_configs,
    get_measurement_values,
)
from utils.style_utils import set_font_lato

# # 05_Measurement_Analysis.py

# Page for viewing measurement distributions and creating measurement feature stores.
#
# Have separated from standardisation page to avoid rerun issues on tab switching.



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



def display_measurement_analysis(snowsesh):
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
            df_values = get_measurement_values(selected_measurement, snowsesh)

        if df_values.empty:
            st.warning(f"No measurement values found for {selected_measurement}")
            return

        df_mapped = apply_unit_mapping(df_values, config)
        unmapped_count = df_mapped['mapped_unit'].isna().sum()

        st.info(f"Loaded {len(df_values):,} measurement values (Unmapped = {unmapped_count:,})")

        df_all = apply_conversions(df_mapped, config)

        fig = create_distribution_plots(df_all, config)
        st.plotly_chart(fig, use_container_width=True)


def display_feature_creation(snowsesh):
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
        create_base_measurements_feature(snowsesh, eligible_configs)

    st.write("""
    **Table Schema:**
    - `PERSON_ID`: Patient identifier
    - `CLINICAL_EFFECTIVE_DATE`: Date of measurement
    - `DEFINITION_ID`: Measurement definition ID
    - `DEFINITION_NAME`: Measurement definition name
    - `SOURCE_RESULT_VALUE`: Original measurement value
    - `SOURCE_RESULT_VALUE_UNITS`: Original measurement unit
    - `VALUE_AS_NUMBER`: Converted value in primary standard unit
    - `VALUE_UNITS`: Primary standard unit (standardized)
    """)

def main():
    st.set_page_config(page_title="Measurement Feature Creation", layout="wide")
    set_font_lato()
    st.title("Measurement Distributions & Feature Creation")
    load_dotenv()

    snowsesh = get_snowflake_connection()

    # Create 2:1 column layout
    left_col, right_col = st.columns([2, 1])

    with left_col:
        display_measurement_analysis(snowsesh)

    with right_col:
        display_feature_creation(snowsesh)


if __name__ == "__main__":
    main()