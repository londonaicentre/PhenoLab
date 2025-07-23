import numpy as np
import pandas as pd
import plotly.express as px
import streamlit as st
from utils.config_utils import load_config
from utils.database_utils import get_snowflake_session
from utils.measurement import MeasurementConfig
from utils.measurement_interaction_utils import (
    apply_conversions,
    apply_unit_mapping,
    count_sigfig,
    get_available_measurement_configs,
    get_measurement_values,
    load_measurement_config,
    load_measurement_configs_into_tables,
    load_measurement_configs_list,
    update_all_measurement_configs,
)
from utils.style_utils import container_object_with_height_if_possible, set_font_lato

# # 04_Measurement_Standardisation.py

# Configuration of unit mappings and conversions to enable standardisation /
# of measurements for relevant definitions. Allows users to define standard units, /
# map source units, and specify conversion formulas.


def display_measurement_analysis(config, tab1 = True, upper_limit = None, lower_limit = None):

    with st.spinner("Loading measurement values..."):
        df_values = get_measurement_values(config.definition_name)

    if df_values.empty:
        st.warning(f"No measurement values found for {config.definition_name}")
        return

    df_mapped = apply_unit_mapping(df_values, config)

    df_all = apply_conversions(df_mapped, config)

    #set form name so they're not the same
    form_name = 'unit_distribution_plot_' + config.definition_name

    if tab1:
        with st.form(form_name):
            st.write('WARNING: increasing the number of rows to pull can be very expensive and slow down the running of this section')
            col1, col2, col3, col4, col5 = st.columns(5)
            local_max_centile = df_all.value.quantile(0.9999)
            local_min_centile = df_all.value.quantile(0.0001)

            with col1:
                xmin = st.number_input(f'XMin ({config.primary_standard_unit})',
                                    value = local_min_centile)

            with col2:
                xmax = st.number_input(f'XMax ({config.primary_standard_unit})',
                                    value = local_max_centile)

            with col3:
                nbinsx = st.number_input('Bins/unit',
                                        value = 1.0,
                                        step = 0.01)

            with col4:
                row_limit = st.number_input("Number of Rows (CAUTION)",
                                            value = 100000)

            with col5:
                plot_submit = st.form_submit_button()

            if plot_submit:
                if row_limit > 100000:
                    df_values = get_measurement_values(config.definition_name, row_limit)
                    df_mapped = apply_unit_mapping(df_values, config)
                    df_all = apply_conversions(df_mapped, config)

                # apply 99.5 percentile cutoff - otherwise extreme outliers will hide true distribution
                #Limit this by the values as this is slowing things down
                above_min = np.where(df_all.converted_value >= xmin)
                below_max = np.where(df_all.converted_value <= xmax)

                unit_distr_plot = px.histogram(
                    df_all.loc[np.intersect1d(above_min, below_max), :],
                        x='converted_value',
                        color = 'mapped_unit',
                        range_x=[xmin, xmax],
                        nbins = round((xmax - xmin)*nbinsx),
                        marginal= 'violin'
                        )
                if upper_limit and lower_limit:
                    unit_distr_plot.add_vrect(x0=lower_limit, x1=upper_limit, line_width=0, fillcolor="green", opacity=0.2)
                elif upper_limit:
                    unit_distr_plot.add_hline(y = upper_limit, col = 'red')
                elif lower_limit:
                    unit_distr_plot.add_hline(y = lower_limit, col = 'red')

                st.plotly_chart(unit_distr_plot, use_container_width=True)

    else:
        with st.form(form_name + 'tab2'):
            plot_submit = st.form_submit_button('Plot distributions')

            if plot_submit:
                xmax = df_all.converted_value.quantile(0.995)
                xmin = df_all.converted_value.quantile(0.005)

                above_min = np.where(df_all.converted_value >= xmin)
                below_max = np.where(df_all.converted_value <= xmax)
                unit_distr_plot = px.histogram(
                            df_all.loc[np.intersect1d(above_min, below_max), :],
                            x='converted_value',
                            color = 'mapped_unit',
                            range_x=[xmin, xmax],
                            labels= {
                                'converted_value': config.primary_standard_unit,
                                'mapped_unit': 'original unit'
                            },
                            nbins = 100,
                            marginal= 'violin',
                            title= f'Distribution of values of {config.definition_name}, converted to units: {config.primary_standard_unit}' #noqa
                            )

                st.plotly_chart(unit_distr_plot, use_container_width=True)


def display_standard_units_panel(config):
    """
    Display panel for choosing and managing standard units.
    These are a set of canonical units onto which source units can map to.
    One of these is chosen as the 'primary' unit for all conversions.
    Rather than maintaining a session state, the config json is updated with every change
    """
    st.subheader("2. Configure Standard Units & Set Primary")

    col1, col2 = st.columns([3, 1])
    with col1:
        new_unit = st.text_input("Add new standard unit")
    with col2:
        st.write("")
        add_button = st.button("Add")

    if add_button and new_unit:
        if config.add_standard_unit(new_unit):
            # if first unit, automatically set as primary
            if len(config.standard_units) == 1:
                config.set_primary_standard_unit(new_unit)
            config.save_to_json(directory=f"data/measurements/{st.session_state.config['icb_name']}")
            st.success(f"Added {new_unit} to standard units")
            st.rerun()
        else:
            st.warning(f"Unit {new_unit} already exists")

    # shows existing units
    if config.standard_units and len(config.standard_units) > 0:
        for i, unit in enumerate(config.standard_units):
            col1, col2, col3 = st.columns([1, 1, 1])
            with col1:
                st.text(unit)
            with col2:
                is_primary = unit == config.primary_standard_unit
                if is_primary:
                    st.write("PRIMARY UNIT")
                else:
                    if st.button("Set Primary", key=f"set_primary_{i}"):
                        config.set_primary_standard_unit(unit)
                        config.save_to_json(directory=f"data/measurements/{st.session_state.config['icb_name']}")
                        st.rerun()
            with col3:
                if st.button("Remove", key=f"remove_unit_{i}"):
                    config.remove_standard_unit(unit)
                    config.save_to_json(directory=f"data/measurements/{st.session_state.config['icb_name']}")
                    st.rerun()
    else:
        st.info("No standard units defined.")

def display_unit_mapping_panel(config):
    """
    Display panel for mapping source units to standard units
    """
    st.subheader("3. Map Source Units to Standard Units")

    if not config.standard_units:
        st.warning("Please add at least one standard unit first.")
        return

    unit_stats_df = pd.DataFrame([{
        'UNIT': m.source_unit,
        'TOTAL_COUNT': m.source_unit_count or 0,
        'NUMERIC_COUNT': m.source_unit_count or 0,
        'LOWER_QUARTILE': m.source_unit_lq,
        'MEDIAN': m.source_unit_median,
        'UPPER_QUARTILE': m.source_unit_uq
    } for m in config.unit_mappings])

    if unit_stats_df.empty:
        st.warning(f"No units found for {config.definition_name} in the configuration file.")
        return

    # creat dict for quick lookup
    current_mappings = {m.source_unit: m.standard_unit for m in config.unit_mappings}

    column_layout = [1, 2, 2, 1, 2]
    header_cols = st.columns(column_layout)
    headers = [
        "**Count**",
        "**Source Unit**",
        "**Median (IQR)**",
        "**%Numeric**",
        "**Target Unit**"
        ]
    for col, header in zip(header_cols, headers, strict=False):
        col.write(header)

    with container_object_with_height_if_possible(600):
        for _, row in unit_stats_df.iterrows():
            source_unit = row['UNIT']
            current_mapping = current_mappings.get(source_unit, "")

            cols = st.columns(column_layout)
            cols[0].write(f"{row['TOTAL_COUNT']}")
            cols[1].write(f"{source_unit}")

            # statistics column
            if pd.notna(row['MEDIAN']):
                # default missing to 0 for display purposes
                lq = row['LOWER_QUARTILE'] if pd.notna(row['LOWER_QUARTILE']) else 0
                median = row['MEDIAN'] if pd.notna(row['MEDIAN']) else 0
                uq = row['UPPER_QUARTILE'] if pd.notna(row['UPPER_QUARTILE']) else 0
                cols[2].write(f"{median:.1f} ({lq:.1f} - {uq:.1f})")
            else:
                cols[2].write("No numeric data")

            # numeric %
            num_percent = row['NUMERIC_COUNT']/row['TOTAL_COUNT']
            cols[3].write(f"{num_percent:.1f}%")

            # target unit selection
            options = [""] + config.standard_units
            index = config.standard_units.index(current_mapping) + 1 if current_mapping in config.standard_units else 0
            selected_standard = cols[4].selectbox(
                "Map to standard unit",
                options=options,
                index=index,
                key=f"mapping_{source_unit}",
                label_visibility="collapsed"
            )

            # update and save if change in mapping
            if selected_standard != current_mapping:
                for mapping in config.unit_mappings:
                    if mapping.source_unit == source_unit:
                        mapping.standard_unit = selected_standard
                        config.mark_modified()
                        break
                config.save_to_json(directory=f"data/measurements/{st.session_state.config['icb_name']}")
                st.rerun()

def get_all_units_for_conversion(config):
    """
    Get all standard units that need conversion to primary unit.
    Source units are not included as they map to standard units (same unit, different representation).
    """
    # Only return standard units (including primary unit)
    return sorted(config.standard_units)

def display_unit_conversion_panel(config):
    """
    Display panel for defining conversions from all units to the primary unit
    """
    st.subheader("5. Define Unit Conversions to Primary Unit")

    if not config.primary_standard_unit:
        st.warning("Please set a primary standard unit first.")
        return

    st.info(f"**Primary Unit**: {config.primary_standard_unit}")
    st.write("Define conversions from all standard units to the primary unit using the formula:")
    st.code("converted_value = (original_value + pre_offset) * multiply_by + post_offset")

    all_units = get_all_units_for_conversion(config)

    if not all_units:
        st.warning("No units available for conversion.")
        return

    existing_conversions = {}
    for conv in config.unit_conversions:
        if conv.convert_to_unit == config.primary_standard_unit:
            existing_conversions[conv.convert_from_unit] = conv

    st.markdown("#### Standard Units to Primary Unit")
    standard_units = [u for u in all_units if u != config.primary_standard_unit]
    if standard_units:
        display_conversion_group(config, standard_units, existing_conversions, "standard")

    st.markdown("#### Primary (source to primary identity conversion)")
    if config.primary_standard_unit:
        display_conversion_group(config, [config.primary_standard_unit], existing_conversions, "identity")

def display_conversion_group(config, units, existing_conversions, group_type):
    """
    Display a group of unit conversions with input fields
    """
    column_layout = [3, 2, 2, 2, 1]
    header_cols = st.columns(column_layout)
    headers = ["**From Unit**", "**Pre-offset**", "**Multiply by**", "**Post-offset**", "**Status**"]
    for col, header in zip(header_cols, headers):
        col.write(header)

    for unit in units:
        cols = st.columns(column_layout)

        cols[0].write(f"{unit} → {config.primary_standard_unit}")

        existing = existing_conversions.get(unit)
        if unit == config.primary_standard_unit:
            # default identity conversio
            pre_offset = 0.0
            multiply_by = 1.0
            post_offset = 0.0
            is_identity = True
        else:
            pre_offset = existing.pre_offset if existing else 0.0
            multiply_by = existing.multiply_by if existing else 1.0
            post_offset = existing.post_offset if existing else 0.0
            is_identity = False

        # Conversion value entry fields
        key_suffix = f"{group_type}_{unit}"
        pre_sigfig = count_sigfig(pre_offset)
        new_pre_offset = cols[1].number_input(
            "Pre-offset",
            value=float(pre_offset),
            key=f"pre_{key_suffix}",
            disabled=is_identity,
            label_visibility="collapsed",
            step = 0.00001,
            format = f"%0.{pre_sigfig}g" if pre_sigfig < 8 else "%0.7g"
        )

        mult_sigfig = count_sigfig(multiply_by)
        new_multiply_by = cols[2].number_input(
            "Multiply by",
            value=float(multiply_by),
            key=f"mult_{key_suffix}",
            disabled=is_identity,
            label_visibility="collapsed",
            step = 0.00001,
            format = f"%0.{mult_sigfig}g" if mult_sigfig < 8 else "%0.7g"
        )
        post_sigfig = count_sigfig(post_offset)
        new_post_offset = cols[3].number_input(
            "Post-offset",
            value=float(post_offset),
            key=f"post_{key_suffix}",
            disabled=is_identity,
            label_visibility="collapsed",
            step = 0.00001,
            format = f"%0.{post_sigfig}g" if post_sigfig < 8 else "%0.7g"
        )

        if is_identity:
            cols[4].write("Identity")
        elif existing and existing.multiply_by != 1.0:
            cols[4].write("Defined")
        else:
            cols[4].write("Default")

        # update if values change
        if not is_identity and (
            new_pre_offset != pre_offset or
            new_multiply_by != multiply_by or
            new_post_offset != post_offset
        ):
            config.add_unit_conversion(
                convert_from_unit=unit,
                convert_to_unit=config.primary_standard_unit,
                pre_offset=new_pre_offset,
                multiply_by=new_multiply_by,
                post_offset=new_post_offset
            )
            config.save_to_json(directory=f"data/measurements/{st.session_state.config['icb_name']}")
            st.rerun()

def display_configs_in_tables():
    measurement_configs = st.session_state.session.sql(f"""
        SELECT DISTINCT
            DEFINITION_NAME,
            CONFIG_ID
        FROM {st.session_state.config["measurement_configs"]["database"]}.
        {st.session_state.config["measurement_configs"]["schema"]}.MEASUREMENT_CONFIGS
        ORDER BY DEFINITION_NAME
        """).to_pandas()

    definition_name = st.selectbox(
        "Select a measurement configuration",
        options=sorted(measurement_configs['DEFINITION_NAME']),
        key="measurement_config_choose"
        )

    config = measurement_configs.loc[measurement_configs['DEFINITION_NAME'] == definition_name, 'CONFIG_ID'].values[0]

    # Get unit mappings
    unit_mappings = st.session_state.session.sql(f"""
        SELECT DISTINCT
            COALESCE(SOURCE_UNIT, 'No Unit') AS SOURCE_UNIT,
            STANDARD_UNIT
        FROM {st.session_state.config["measurement_configs"]["database"]}.
        {st.session_state.config["measurement_configs"]["schema"]}.UNIT_MAPPINGS
        WHERE CONFIG_ID = '{config}'
        ORDER BY SOURCE_UNIT
        """).to_pandas()

    # Get primary unit
    primary_unit_df = st.session_state.session.sql(f"""
        SELECT DISTINCT
            UNIT
        FROM {st.session_state.config["measurement_configs"]["database"]}.
        {st.session_state.config["measurement_configs"]["schema"]}.STANDARD_UNITS
            WHERE CONFIG_ID = '{config}'
            AND PRIMARY_UNIT = TRUE
        """).to_pandas()

    primary_unit = primary_unit_df['UNIT'].iloc[0] if not primary_unit_df.empty else 'Not set'

    # Get value bounds
    value_bounds = st.session_state.session.sql(f"""
        SELECT
            LOWER_LIMIT,
            UPPER_LIMIT
        FROM {st.session_state.config["measurement_configs"]["database"]}.
        {st.session_state.config["measurement_configs"]["schema"]}.VALUE_BOUNDS
            WHERE CONFIG_ID = '{config}'
        """).to_pandas()

    if not value_bounds.empty:
        upper_limit = value_bounds.UPPER_LIMIT.iloc[0]
        lower_limit = value_bounds.LOWER_LIMIT.iloc[0]
    else:
        upper_limit = np.nan
        lower_limit = np.nan

    # Get measurement statistics
    total_measurements = st.session_state.session.sql(f"""
        SELECT
            SUM(SOURCE_UNIT_COUNT)
        FROM {st.session_state.config["measurement_configs"]["database"]}.
        {st.session_state.config["measurement_configs"]["schema"]}.UNIT_MAPPINGS
            WHERE CONFIG_ID = '{config}'
        """).to_pandas()['SUM(SOURCE_UNIT_COUNT)'].iloc[0]

    mapped_measurements = st.session_state.session.sql(f"""
        SELECT
            SUM(SOURCE_UNIT_COUNT) AS MAPPED_COUNT
        FROM {st.session_state.config["measurement_configs"]["database"]}.
        {st.session_state.config["measurement_configs"]["schema"]}.UNIT_MAPPINGS
            WHERE CONFIG_ID = '{config}'
            AND STANDARD_UNIT IS NOT NULL
            AND STANDARD_UNIT != ''
        """).to_pandas()['MAPPED_COUNT'].iloc[0]

    if not mapped_measurements:
        mapped_measurements = 0

    # Display all statistics together
    # First row - configuration settings
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Primary Unit", primary_unit)
    with col2:
        st.metric("Lower Limit", f"{lower_limit:.2f}" if not np.isnan(lower_limit) else "Not set")
    with col3:
        st.metric("Upper Limit", f"{upper_limit:.2f}" if not np.isnan(upper_limit) else "Not set")

    # Second row - mapping statistics
    metric1, metric2, metric3 = st.columns(3)
    with metric1:
        st.metric('Total Measurements', f"{total_measurements:,}")
    with metric2:
        st.metric('Mapped Measurements', f"{mapped_measurements:,}")
    with metric3:
        proportion_mapped = round(mapped_measurements/total_measurements*100) if total_measurements > 0 else 0
        st.metric('Proportion Mapped', f'{proportion_mapped}%')

    # unit mappings table
    if not unit_mappings.empty:
        mapped_units = unit_mappings[unit_mappings['STANDARD_UNIT'].notna() & (unit_mappings['STANDARD_UNIT'] != '')]
        if not mapped_units.empty:
            st.dataframe(
                mapped_units[['SOURCE_UNIT', 'STANDARD_UNIT']].rename(columns={
                    'SOURCE_UNIT': 'Source Unit',
                    'STANDARD_UNIT': 'Standard Unit'
                }),
                use_container_width=True,
                hide_index=True
            )
        else:
            st.info("No unit mappings configured yet.")
    else:
        st.info("No units found for this measurement.")

    if np.isnan(upper_limit):
        upper_limit = None
    if np.isnan(lower_limit):
        lower_limit = None

    return definition_name, upper_limit, lower_limit

def display_measurement_bounds_panel(config: MeasurementConfig):
    st.subheader("6. Define Value Bounds")

    st.write(f"""Define reasonable bounds for {config.definition_name} values in {config.primary_standard_unit}.
    Values outside these bounds will be flagged but not deleted, allowing you to identify potential data quality issues.""")

    new_lower_bound = st.number_input(
        f"Lower bound for values in {config.primary_standard_unit}",
        value=config.lower_limit if hasattr(config, 'lower_limit') else None,
        placeholder='e.g. 0.0',
    )
    if new_lower_bound:
        if new_lower_bound != config.lower_limit:
            config.add_lower_bound(new_lower_bound)
            config.save_to_json(directory=f"data/measurements/{st.session_state.config['icb_name']}")
            st.success("Lower bound set successfully.")

    new_upper_bound = st.number_input(
        f"Upper bound for values in {config.primary_standard_unit}",
        value=config.upper_limit if hasattr(config, 'upper_limit') else None,
        placeholder='e.g. 140.0',
    )
    if new_upper_bound:
        if new_upper_bound != config.upper_limit:
            config.add_upper_bound(new_upper_bound)
            config.save_to_json(directory=f"data/measurements/{st.session_state.config['icb_name']}")
            st.success("Upper bound set successfully.")

def get_selected_config(selected_measurement: str):
    config_options = get_available_measurement_configs()

    if not config_options:
        st.warning("No measurement configurations with standard units defined. " \
        "Please define standard units in the Measurement Standardisation page first.")
        return

    if selected_measurement:
        config = config_options[selected_measurement]

        if not config.primary_standard_unit:
            st.warning(f"No primary unit set for {selected_measurement}." \
                       "Please set a primary unit in the Measurement Standardisation page.")
            return

        else:
            return config


def main():  # noqa: C901
    st.set_page_config(page_title="Standardise Measurements", layout="wide")
    set_font_lato()
    if "session" not in st.session_state:
        st.session_state.session = get_snowflake_session()
    if "config" not in st.session_state:
        st.session_state.config = load_config()
    st.title("Standardise Measurements")

    if "selected_definition" not in st.session_state:
        st.session_state.selected_definition = None
        st.session_state.selected_config = None

    if st.session_state.config["local_development"]:
        tab1, tab2 = st.tabs(["Create/Update Configs", "View Existing Configs on Snowflake"])
        with tab1:
            col1, col2 = st.columns([3, 1])
            with col1:
                st.write("""
                This page allows superusers to standardise and clean measurement values - individual to each ICB.
                """)

                st.write("""
                Run 'Update Config Stats' to include new measurement definitions. Definitions must be in Snowflake:
                - Creates new configs for any measurement definitions that don't have one
                - For each measurement config, will load in all source units and statistics
                - If units already exist in the config, it will load in newly discovered units only
                """)

                with st.expander("What happens when you click 'Update Config Stats'?"):
                    st.markdown("""
                    1. **Scans for new measurement definitions** in **Snowflake** that don't yet have a configuration file
                       - Queries the DEFINITIONSTORE for measurement definitions
                       - Creates new JSON config files in `data/measurements/{icb_name}/` for each

                    2. **Updates all configurations** with usage statistics from live data:
                       - Queries measurement values from observation tables to find all unique source units
                       - Calculates statistics for each unit (count, median, quartiles)
                       - Adds any newly discovered units to existing configs

                    3. **Updates are local only** until you click "Send configs to Snowflake"
                       - All changes are saved to local JSON files
                       - Changes should be reviewed before pushing to Snowflake tables

                    **Note**: This process can take a few minutes if there are many measurements to analyse.
                    """)
            with col2:
                if st.button("Update Config Stats", use_container_width=True):
                    with st.spinner("Updating measurement configurations..."):
                        created, updated, new_units = update_all_measurement_configs()

                        message_parts = []
                        if created > 0:
                            message_parts.append(f"Created {created} new configuration{'s' if created > 1 else ''}")
                        if updated > 0:
                            message_parts.append(f"Updated {updated} existing configuration{'s' if updated > 1 else ''}")
                        if new_units > 0:
                            message_parts.append(f"Added {new_units} new source unit{'s' if new_units > 1 else ''}")

                        if message_parts:
                            st.success(". ".join(message_parts))
                        else:
                            st.info("No changes were necessary")

            st.markdown("---")

            st.subheader("1. Select Measurement Config File")

            # 1. refresh configs
            measurement_configs = load_measurement_configs_list()
            if not measurement_configs:
                st.write(st.session_state.config['icb_name'])
                st.warning("No measurement configurations found. Please check data/measurements/<icb_name>.")
                return

            # 2. map definition name to filename
            config_by_name = {}

            for config_file in measurement_configs:
                try:
                    config = load_measurement_config(config_file)
                    if config:
                        config_by_name[config.definition_name] = config_file
                except Exception as e:
                    st.error(f"Error loading {config_file}: {e}")
                    pass

            if not config_by_name:
                st.warning("Could not load any valid measurement configurations.")
                return

            # 4. selection + load
            selected_def_name = st.selectbox(
                "Select a measurement configuration",
                options=sorted(config_by_name.keys()),
                key="measurement_config_select"
            )

            if selected_def_name:
                config_filename = config_by_name[selected_def_name]
                config = load_measurement_config(config_filename)

                if config:
                    st.session_state.selected_definition = selected_def_name
                    st.session_state.selected_config = config

                    st.info(f"**Selected Configuration**: {config.definition_name}")
                    st.markdown("---")

                    # UI: standard units selection panel
                    display_standard_units_panel(config)
                    st.markdown("---")

                    # UI: unit mapping panel
                    if config.standard_units:
                        display_unit_mapping_panel(config)

                        # UI: view unit distributions
                        if config.standard_units:
                            st.markdown("---")
                            st.subheader("4. View Unit Distributions")
                            display_measurement_analysis(config)

                        # UI: unit conversion panel
                        if config.primary_standard_unit:
                            st.markdown("---")
                            display_unit_conversion_panel(config)

                        # UI: value bounds panel
                        if config.primary_standard_unit:
                            st.markdown("---")
                            display_measurement_bounds_panel(config)
                    else:
                        st.info("Please add standard units first to enable unit mapping.")

            st.markdown("---")
            st.subheader("7. Update Measurement Configs on Snowflake")
            if st.button("Send configs to Snowflake"):
                with st.spinner("Sending configs to Snowflake..."):
                    # show target tables (dev vs prod)
                    db = st.session_state.config["measurement_configs"]["database"]
                    schema = st.session_state.config["measurement_configs"]["schema"]
                    st.info(f"**Target tables:** {db}.{schema}.[MEASUREMENT_CONFIGS, STANDARD_UNITS, UNIT_MAPPINGS, UNIT_CONVERSIONS, VALUE_BOUNDS]")

                    total_configs = load_measurement_configs_into_tables()
                    st.success(f"Successfully uploaded {total_configs} measurement configs to Snowflake!")
        with tab2:
            selected_measurement, ulim, llim  = display_configs_in_tables()
            measurement_config = get_selected_config(selected_measurement)
            if measurement_config:
                display_measurement_analysis(measurement_config, False, ulim, llim)
    else:
        selected_measurement, ulim, llim = display_configs_in_tables()
        measurement_config = get_selected_config(selected_measurement)
        if measurement_config:
            display_measurement_analysis(measurement_config, False, ulim, llim)





if __name__ == "__main__":
    main()
