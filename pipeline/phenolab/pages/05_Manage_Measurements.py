import os
from typing import Dict, List, Optional, Tuple

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from dotenv import load_dotenv
from plotly.subplots import make_subplots
from utils.database_utils import get_snowflake_connection
from utils.measurement import MeasurementConfig, UnitMapping, load_measurement_config_from_json
from utils.style_utils import set_font_lato

from phmlondon.config import DDS_OBSERVATION, DEFINITION_LIBRARY, FEATURE_METADATA, FEATURE_STORE, SNOWFLAKE_DATABASE
from phmlondon.definition import Definition
from phmlondon.feature_store_manager import FeatureStoreManager
from phmlondon.snow_utils import SnowflakeConnection

# # 05_Measurement_Analysis.py

# Page for viewing measurement distributions and creating measurement feature stores.
#
# Have separated from standardisation page to avoid rerun issues on tab switching.


def load_measurement_configs_list():
    """
    Get list of measurement config files from /data/measurements
    """
    config_list = []
    try:
        if os.path.exists("data/measurements"):
            config_list = [f for f in os.listdir("data/measurements")
                           if f.endswith(".json") and f.startswith("standard_")]
    except Exception as e:
        st.error(f"Unable to list measurement config files: {e}")

    return config_list


def load_measurement_config(filename):
    """
    Load measurement config from json
    """
    try:
        file_path = os.path.join("data/measurements", filename)
        config = load_measurement_config_from_json(file_path)
        return config
    except Exception as e:
        st.error(f"Unable to load measurement config: {e}")
        return None


@st.cache_data(ttl=600, show_spinner="Loading measurement values...")
def get_measurement_values(definition_name: str, _snowsesh: SnowflakeConnection) -> pd.DataFrame:
    """
    Get actual measurement values for a definition
    """
    query = f"""
    SELECT
        RESULT_VALUE_UNITS AS unit,
        TRY_CAST(RESULT_VALUE AS FLOAT) AS value
    FROM {DDS_OBSERVATION} obs
    LEFT JOIN {SNOWFLAKE_DATABASE}.{DEFINITION_LIBRARY}.DEFINITIONSTORE def
        ON obs.CORE_CONCEPT_ID = def.DBID
    WHERE def.DEFINITION_NAME = '{definition_name}'
        AND RESULT_VALUE IS NOT NULL
        AND TRY_CAST(RESULT_VALUE AS FLOAT) IS NOT NULL
    LIMIT 100000 -- gets decent sample
    """
    df = _snowsesh.execute_query_to_df(query)
    df.columns = df.columns.str.lower()
    return df


def apply_unit_mapping(df, config: MeasurementConfig):
    """
    Apply unit mappings to convert source units to standard units
    """
    df_mapped = df.copy()

    unit_mapping_dict = {m.source_unit: m.standard_unit for m in config.unit_mappings if m.standard_unit}

    df_mapped['mapped_unit'] = df_mapped['unit'].map(unit_mapping_dict)

    return df_mapped


def apply_conversions(df, config: MeasurementConfig):
    """
    Apply conversions to convert all source values to value per primary unit
    """
    if not config.primary_standard_unit:
        return pd.DataFrame()

    df_converted = df.copy()
    df_converted['converted_value'] = df_converted['value']
    df_converted['converted_unit'] = config.primary_standard_unit

    conversion_dict = {}
    for conv in config.unit_conversions:
        if conv.convert_to_unit == config.primary_standard_unit:
            conversion_dict[conv.convert_from_unit] = conv

    for idx, row in df_converted.iterrows():
        unit_to_convert = row['mapped_unit'] if pd.notna(row['mapped_unit']) else row['unit']

        if unit_to_convert in conversion_dict:
            conv = conversion_dict[unit_to_convert]
            original_value = row['value']
            converted_value = (original_value + conv.pre_offset) * conv.multiply_by + conv.post_offset
            df_converted.at[idx, 'converted_value'] = converted_value
        elif unit_to_convert == config.primary_standard_unit:
            # if already in primary unit
            df_converted.at[idx, 'converted_value'] = row['value']

    return df_converted


def create_distribution_plots(df_all, config: MeasurementConfig, selected_standard_unit: str = None):
    """
    Create four distribution plots in a 2x2 grid: unmapped, selected standard unit, primary unit, and percentiles
    """
    # apply 99.5 percentile cutoff - otherwise extreme outliers will hide true distribution
    percentile_995 = df_all['value'].quantile(0.995) if not df_all.empty else float('inf')
    df_all_filtered = df_all[df_all['value'] <= percentile_995].copy()

    if 'converted_value' in df_all_filtered.columns:
        converted_percentile_995 = df_all_filtered['converted_value'].quantile(0.995) if not df_all_filtered.empty else float('inf')
        df_all_filtered = df_all_filtered[df_all_filtered['converted_value'] <= converted_percentile_995]

    df_unmapped = df_all_filtered[df_all_filtered['mapped_unit'].isna()]
    if selected_standard_unit:
        df_standard = df_all_filtered[df_all_filtered['mapped_unit'] == selected_standard_unit]
    else:
        df_standard = pd.DataFrame()  # keep this empty if not yet selected

    df_primary = df_all_filtered[['converted_value', 'converted_unit']].rename(columns={'converted_value': 'value', 'converted_unit': 'unit'})

    # Create 2x2 subplot grid
    fig = make_subplots(
        rows=2, cols=2,
        subplot_titles=(
            f"Unmapped Units ({len(df_unmapped):,} values)",
            f"Standard Unit: {selected_standard_unit or 'None Selected'} ({len(df_standard):,} values)",
            f"Primary Unit: {config.primary_standard_unit} ({len(df_primary):,} values)",
            f"Percentile Distribution ({config.primary_standard_unit})"
        ),
        vertical_spacing=0.15,
        horizontal_spacing=0.12,
        specs=[[{"type": "histogram"}, {"type": "histogram"}],
               [{"type": "histogram"}, {"type": "scatter"}]]
    )

    # plot 1: unmapped (top left)
    if not df_unmapped.empty:
        fig.add_trace(
            go.Histogram(x=df_unmapped['value'], name="All Unmapped", nbinsx=50, marker_color='lightgray'),
            row=1, col=1
        )
    else:
        fig.add_annotation(
            text="No unmapped units",
            xref="x1", yref="y1",
            x=0.5, y=0.5,
            showarrow=False,
            row=1, col=1
        )

    # plot 2: selected standard (top right)
    if not df_standard.empty and selected_standard_unit:
        fig.add_trace(
            go.Histogram(x=df_standard['value'], name=selected_standard_unit, nbinsx=50, marker_color='steelblue'),
            row=1, col=2
        )
    else:
        fig.add_annotation(
            text="Select a standard unit" if not selected_standard_unit else "No data",
            xref="x2", yref="y2",
            x=0.5, y=0.5,
            showarrow=False,
            row=1, col=2
        )

    # plot 3: primary (bottom left)
    if not df_primary.empty and config.primary_standard_unit:
        fig.add_trace(
            go.Histogram(x=df_primary['value'], name=config.primary_standard_unit, nbinsx=50, marker_color='darkgreen'),
            row=2, col=1
        )

    # plot 4: percentiles (bottom right)
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
            ), row=2, col=2)

            # draw Median
            median_idx = percentiles.index(0.50)
            fig.add_hline(
                y=percentile_values[median_idx],
                line_dash="dash",
                line_color="gray",
                annotation_text=f"Median: {percentile_values[median_idx]:.2f}",
                row=2, col=2
            )

    # update axes
    fig.update_xaxes(title_text="Value", row=1, col=1)
    fig.update_xaxes(title_text="Value", row=1, col=2)
    fig.update_xaxes(title_text="Value", row=2, col=1)
    fig.update_xaxes(title_text="Percentile", row=2, col=2)

    fig.update_yaxes(title_text="Count", row=1, col=1)
    fig.update_yaxes(title_text="Count", row=1, col=2)
    fig.update_yaxes(title_text="Count", row=2, col=1)
    fig.update_yaxes(title_text=f"Value ({config.primary_standard_unit})", row=2, col=2)

    # update layout
    fig.update_layout(
        height=700,
        showlegend=False,
        title_text=f"Measurement Value Distributions: {config.definition_name} (99.5 percentile cutoff applied)"
    )

    return fig


def get_available_measurement_configs():
    """
    Get measurement configs that are available for both distribution analysis and feature creation.
    Used by both "View Distributions" and "Create Measurement Features" tabs.

    Requirements:
    - Have standard units defined
    - Have a primary standard unit set
    - Have unit mappings defined


    """
    measurement_configs = load_measurement_configs_list()
    available_configs = {}

    for config_file in measurement_configs:
        try:
            config = load_measurement_config(config_file)
            if (config and
                config.standard_units and  # Has standard units defined
                config.primary_standard_unit and  # Has primary unit set
                config.unit_mappings):  # Has unit mappings
                available_configs[config.definition_name] = config
        except Exception as e:
            st.warning(f"Error loading config {config_file}: {e}")
            continue

    return available_configs


def display_measurement_analysis(snowsesh):
    """
    Display measurement analysis with distribution visualizations
    """
    config_options = get_available_measurement_configs()

    if not config_options:
        st.warning("No measurement configurations with standard units defined. " \
        "Please define standard units in the Measurement Standardisation page first.")
        return

    # UI: select measurement dropdown
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

        st.info(f"Loaded {len(df_values):,} measurement values")

        df_mapped = apply_unit_mapping(df_values, config)
        df_all = apply_conversions(df_mapped, config)

        standard_units_in_data = df_all[df_all['mapped_unit'].notna()]['mapped_unit'].unique()
        standard_units_list = sorted([unit for unit in standard_units_in_data if pd.notna(unit)])

        if standard_units_list:
            selected_standard_unit = st.selectbox(
                "Select a standard unit to display",
                options=[""] + standard_units_list,
                key="standard_unit_display_select"
            )
            if selected_standard_unit == "":
                selected_standard_unit = None
        else:
            selected_standard_unit = None
            st.info("No mapped standard units found in the data")

        fig = create_distribution_plots(df_all, config, selected_standard_unit)
        st.plotly_chart(fig, use_container_width=True)


def create_base_measurements_sql(eligible_configs: Dict[str, MeasurementConfig]) -> str:
    """
    Generate SQL query dynamically for the Base Measurements feature table
    From array of measurement configurations
    """
    union_queries = []

    for definition_name, config in eligible_configs.items():
        # for CASE statement, source to standard lookup
        unit_mappings = {m.source_unit: m.standard_unit for m in config.unit_mappings if m.standard_unit}

        mapped_standard_units = set(unit_mappings.values())

        conversion_cases = []

        # 1. take unique explicit conversions (i.e. pre, multiply, post-offset)
        explicit_conversions = set()
        for conv in config.unit_conversions:
            if conv.convert_to_unit == config.primary_standard_unit:
                conversion_cases.append(f"""
                    WHEN mapped_unit = '{conv.convert_from_unit}' THEN
                        (({conv.pre_offset} + TRY_CAST(source_result_value AS FLOAT)) * {conv.multiply_by}) + {conv.post_offset}
                """)
                explicit_conversions.add(conv.convert_from_unit)

        # 2. take primary unit values without explicit conversion (i.e. try cast and take value)
        for standard_unit in mapped_standard_units:
            if standard_unit not in explicit_conversions:
                conversion_cases.append(f"""
                    WHEN mapped_unit = '{standard_unit}' THEN
                        TRY_CAST(source_result_value AS FLOAT)
                """)

        # 3. create unit mapping CASE statement
        mapping_cases = []
        for source_unit, standard_unit in unit_mappings.items():
            mapping_cases.append(f"WHEN source_result_value_units = '{source_unit}' THEN '{standard_unit}'")

        if not mapping_cases:
            continue  # skip it no mappings

        mapping_case_sql = f"""
            CASE
                {' '.join(mapping_cases)}
                ELSE NULL
            END
        """

        if not conversion_cases:
            continue  # skip if no conversions (should always be a primary one)

        conversion_case_sql = f"""
            CASE
                {' '.join(conversion_cases)}
                ELSE NULL
            END
        """

        # 4. create query for each conversion
        query = f"""
        SELECT
            obs.PERSON_ID,
            obs.CLINICAL_EFFECTIVE_DATE,
            def.DEFINITION_ID,
            def.DEFINITION_NAME,
            obs.RESULT_VALUE AS SOURCE_RESULT_VALUE,
            obs.RESULT_VALUE_UNITS AS SOURCE_RESULT_VALUE_UNITS,
            {conversion_case_sql.replace('mapped_unit', f'({mapping_case_sql})')} AS VALUE_AS_NUMBER,
            '{config.primary_standard_unit}' AS VALUE_UNITS
        FROM {DDS_OBSERVATION} obs
        LEFT JOIN {SNOWFLAKE_DATABASE}.{DEFINITION_LIBRARY}.DEFINITIONSTORE def
            ON obs.CORE_CONCEPT_ID = def.DBID
        WHERE def.DEFINITION_NAME = '{definition_name}'
            AND obs.RESULT_VALUE IS NOT NULL
            AND TRY_CAST(obs.RESULT_VALUE AS FLOAT) IS NOT NULL
            AND obs.RESULT_VALUE_UNITS IS NOT NULL
            AND ({mapping_case_sql}) IS NOT NULL  -- Only include mappable units
            AND ({conversion_case_sql.replace('mapped_unit', f'({mapping_case_sql})')}) IS NOT NULL  -- Only include convertible values
        """

        union_queries.append(query)

    if not union_queries:
        return None

    final_query = " UNION ALL ".join(union_queries)

    return final_query


def display_feature_creation(snowsesh):
    """
    Display simplified measurement feature creation interface
    """
    eligible_configs = get_available_measurement_configs()

    if not eligible_configs:
        st.warning("No measurement configurations found. Please ensure measurements have standard units defined, primary standard unit is set, and unit mappings are defined.")
        return

    st.subheader("Create Base Measurements Feature Table")

    if st.button("Create / Update Base Measurements Table", type="primary", use_container_width=True):
        create_base_measurements_feature(snowsesh, eligible_configs)
    st.write("""
    This will create or update the **Base Measurements** feature table containing standardised measurement data.

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


def create_base_measurements_feature(snowsesh: SnowflakeConnection, eligible_configs: Dict[str, MeasurementConfig]):
    """
    Create the Base Measurements feature table using FeatureStoreManager
    """
    try:
        with st.spinner("Generating SQL query..."):
            sql_query = create_base_measurements_sql(eligible_configs)

        if not sql_query:
            st.error("Failed to generate SQL query. No eligible measurements found.")
            return

        with st.spinner("Initialising Feature Store Manager..."):
            feature_manager = FeatureStoreManager(
                connection=snowsesh,
                database=SNOWFLAKE_DATABASE,
                schema=FEATURE_STORE,
                metadata_schema=FEATURE_METADATA
            )

        feature_name = "BASE_MEASUREMENTS"
        feature_desc = f"Standardised measuremeznt data from {len(eligible_configs)} measurement definitions with unit conversions applied"
        feature_format = "tabular"

        with st.spinner("Creating or updating Base Measurements feature table..."):
            session = snowsesh.session
            with snowsesh.use_context(database=SNOWFLAKE_DATABASE, schema=FEATURE_METADATA):
                feature_id_result = session.sql(f"""
                    SELECT feature_id FROM feature_registry
                    WHERE feature_name = '{feature_name}'
                """).collect()

            if feature_id_result:
                st.info("Feature already exists. Updating with new data...")
                existing_feature_id = feature_id_result[0]["FEATURE_ID"]

                feature_version, table_name = feature_manager.update_feature(
                    feature_id=existing_feature_id,
                    new_sql_select_query=sql_query,
                    change_description=f"Updated with {len(eligible_configs)} measurement definitions",
                    force_new_version=True
                )

                st.success(f"Base Measurements feature updated successfully!")
                st.write(f"**Feature ID:** {existing_feature_id}")
                st.write(f"**New Version:** {feature_version}")
                st.write(f"**Table Name:** {table_name}")
            else:
                feature_id, feature_version = feature_manager.add_new_feature(
                    feature_name=feature_name,
                    feature_desc=feature_desc,
                    feature_format=feature_format,
                    sql_select_query_to_generate_feature=sql_query
                )

                st.success(f"Base Measurements feature created successfully!")
                st.write(f"**Feature ID:** {feature_id}")
                st.write(f"**Feature Version:** {feature_version}")

                table_name = f"{feature_name}_V{feature_version}"
                st.write(f"**Table Name:** {table_name}")

            # get row count for both cases
            try:
                with snowsesh.use_context(database=SNOWFLAKE_DATABASE, schema=FEATURE_STORE):
                    count_result = snowsesh.execute_query_to_df(f"SELECT COUNT(*) as row_count FROM {table_name}")
                    row_count = count_result.iloc[0]['ROW_COUNT']
                    st.write(f"**Rows Created:** {row_count:,}")
            except Exception as e:
                st.warning(f"Could not get row count: {e}")

    except Exception as e:
        st.error(f"Error creating Base Measurements feature: {e}")
        st.exception(e)


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