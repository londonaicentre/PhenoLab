import pandas as pd
import streamlit as st
from dotenv import load_dotenv
# from utils.database_utils import get_snowflake_connection
from utils.database_utils import get_snowflake_session
from utils.measurement import MeasurementConfig
from utils.measurement_interaction_utils import (
    load_measurement_config,
    load_measurement_configs_list,
    update_all_measurement_configs,
)
from utils.style_utils import set_font_lato, container_object_with_height_if_possible

# # 04_Measurement_Standardisation.py

# Configuration of unit mappings and conversions to enable standardisation /
# of measurements for relevant definitions. Allows users to define standard units, /
# map source units, and specify conversion formulas.

# TO DO
# - Not yet feature complete!



def display_standard_units_panel(config):
    """
    Display panel for choosing and managing standard units.
    These are a set of canonical units onto which source units can map to.
    One of these is chosen as the 'primary' unit for all conversions.
    Rather than maintaining a session state, the config json is updated with every change
    """
    st.subheader("Configure Standard Units")

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
            config.save_to_json()
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
                        config.save_to_json()
                        st.rerun()
            with col3:
                if st.button("Remove", key=f"remove_unit_{i}"):
                    config.remove_standard_unit(unit)
                    config.save_to_json()
                    st.rerun()
    else:
        st.info("No standard units defined.")

def display_unit_mapping_panel(config):
    """
    Display panel for mapping source units to standard units
    """
    st.subheader("Map Source Units to Standard Units")

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

    column_layout = [1, 2, 2, 1, 2, 1]
    header_cols = st.columns(column_layout)
    headers = [
        "**Count**",
        "**Source Unit**",
        "**Median (IQR)**",
        "**%Numeric**",
        "**Target Unit**",
        "**Plot Distribution**"
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
                config.save_to_json()
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
    st.subheader("Define Unit Conversions to Primary Unit")

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

    st.markdown("### Standard Units to Primary Unit")
    standard_units = [u for u in all_units if u != config.primary_standard_unit]
    if standard_units:
        display_conversion_group(config, standard_units, existing_conversions, "standard")

    st.markdown("### Primary (identity conversion)")
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
        new_pre_offset = cols[1].number_input(
            "Pre-offset",
            value=float(pre_offset),
            key=f"pre_{key_suffix}",
            disabled=is_identity,
            label_visibility="collapsed"
        )

        new_multiply_by = cols[2].number_input(
            "Multiply by",
            value=float(multiply_by),
            key=f"mult_{key_suffix}",
            disabled=is_identity,
            label_visibility="collapsed"
        )

        new_post_offset = cols[3].number_input(
            "Post-offset",
            value=float(post_offset),
            key=f"post_{key_suffix}",
            disabled=is_identity,
            label_visibility="collapsed"
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
            config.save_to_json()
            st.rerun()

def main():
    st.set_page_config(page_title="Standardise Measurements", layout="wide")
    set_font_lato()
    st.title("Standardise Measurements")
    load_dotenv()

    # snowsesh = get_snowflake_connection()
    session = get_snowflake_session()

    if "selected_definition" not in st.session_state:
        st.session_state.selected_definition = None
        st.session_state.selected_config = None

    col1, col2 = st.columns([3, 1])
    with col1:
        st.write("""
        Update Measurement Configs from new definitions and usage statistics:
        - Creates new configs for any measurement definitions that don't have one
        - For each measurement config, will load in all source units and statistics
        - If units already exist in the config, it will load in newly discovered units only
        """)
    with col2:
        if st.button("Update All Configs", use_container_width=True):
            with st.spinner("Updating measurement configurations..."):
                created, updated, new_units = update_all_measurement_configs(session, st.session_state.config)

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

    st.subheader("Select Measurement Config File")

    # 1. refresh configs
    measurement_configs = load_measurement_configs_list()
    if not measurement_configs:
        st.warning("No measurement configurations found. Please check data/measurements.")
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

                # UI: unit conversion panel
                if config.primary_standard_unit:
                    st.markdown("---")
                    display_unit_conversion_panel(config)
            else:
                st.info("Please add standard units first to enable unit mapping.")

if __name__ == "__main__":
    main()