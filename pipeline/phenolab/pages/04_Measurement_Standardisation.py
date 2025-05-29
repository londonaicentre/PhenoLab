import os
import streamlit as st
from dotenv import load_dotenv
from typing import List, Optional, Tuple
import glob
import pandas as pd

from phmlondon.definition import Definition
from phmlondon.snow_utils import SnowflakeConnection
from utils.style_utils import set_font_lato
from utils.definition_display_utils import load_definition, load_definitions_list
from utils.database_utils import connect_to_snowflake, get_measurement_unit_statistics

from utils.measurement import MeasurementConfig, UnitMapping, load_measurement_config_from_json
from phmlondon.config import SNOWFLAKE_DATABASE, DEFINITION_LIBRARY


# # 04_Measurement_Standardisation.py

# Configuration of unit mappings and conversions to enable standardisation /
# of measurements for relevant definitions. Allows users to define standard units, /
# map source units, and specify conversion formulas.

# TO DO
# - Not yet feature complete!


def load_measurement_definitions_list() -> List[str]:
    """
    Get list of definition files from /data/definitions that start with 'measurement_'
    """
    definitions_list = []
    try:
        if os.path.exists("data/definitions"):
            definitions_list = [f for f in os.listdir("data/definitions")
                               if f.endswith(".json") and "measurement_" in f]
    except Exception as e:
        st.error(f"Unable to list measurement definition files: {e}")

    return definitions_list

def load_measurement_configs_list() -> List[str]:
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

def load_measurement_config(filename: str) -> Optional[MeasurementConfig]:
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

def create_missing_measurement_configs():
    """
    Check if each measurement_ definition has a corresponding standard_ config
    Automagically create empty ones if missing
    """
    os.makedirs("data/measurements", exist_ok=True)

    measurement_definitions = load_measurement_definitions_list()
    measurement_configs = load_measurement_configs_list()

    # 1. get definition ids from existing configs
    config_definition_ids = set()
    for config_file in measurement_configs:
        try:
            config = load_measurement_config(config_file)
            if config:
                config_definition_ids.add(config.definition_id)
        except Exception as e:
            st.warning(f"Could not load configuration file {config_file}: {e}")

    # 2. create blank configs for measurements that are missing
    created_count = 0
    for def_file in measurement_definitions:
        try:
            file_path = os.path.join("data/definitions", def_file)
            definition = load_definition(file_path)

            if definition and definition.definition_id not in config_definition_ids:
                config = MeasurementConfig(
                    definition_id=definition.definition_id,
                    definition_name=definition.definition_name,
                    standard_measurement_config_id=None,  # generated post_init
                    standard_measurement_config_version=None,  # generated post_init
                )
                config.save_to_json()
                created_count += 1

        except Exception as e:
            st.warning(f"Could not process {def_file}: {e}")

    return created_count

def update_all_measurement_configs(snowsesh: SnowflakeConnection) -> Tuple[int, int, int]:
    """
    Button-triggered function to:
    1. Create missing measurement configs for any measurement_ definitions
    2. Only add new source units that don't exist in the config yet
    Note that previously retrieved stats are not updated.

    Returns:
        Tuple[int, int, int]:
            - count of new configs created
            - count of configs updated
            - count of new units added
    """
    os.makedirs("data/measurements", exist_ok=True)

    # track for reporting
    created_count = 0
    updated_count = 0
    new_units_count = 0

    # 1) create missing measurement configs
    missing_created = create_missing_measurement_configs()
    created_count += missing_created

    # 2) get all existing measurement config files
    existing_configs = {}
    for config_file in load_measurement_configs_list():
        try:
            config = load_measurement_config(config_file)
            if config:
                existing_configs[config.definition_name] = config
        except Exception as e:
            st.warning(f"Could not load config {config_file}: {e}")

    # 3) per config, add only new units that don't exist yet
    for def_name, config in existing_configs.items():
        try:
            unit_stats = get_measurement_unit_statistics(def_name, snowsesh)

            if unit_stats is None or unit_stats.empty:
                continue

            existing_source_units = {m.source_unit for m in config.unit_mappings}
            config_changed = False

            for idx, row in unit_stats.iterrows():
                source_unit = row['UNIT']

                # add units that don't exist in the config with empty mappings
                if source_unit not in existing_source_units:
                    config.unit_mappings.append(UnitMapping(
                        source_unit=source_unit,
                        standard_unit="",
                        source_unit_count=row['TOTAL_COUNT'],
                        source_unit_lq=row['LOWER_QUARTILE'],
                        source_unit_median=row['MEDIAN'],
                        source_unit_uq=row['UPPER_QUARTILE']
                    ))
                    new_units_count += 1
                    config_changed = True

            # save if new units were added
            if config_changed:
                config.mark_modified()
                config.save_to_json()
                updated_count += 1

        except Exception as e:
            st.warning(f"Error processing {def_name}: {e}")

    return created_count, updated_count, new_units_count

def display_standard_units_panel(config: MeasurementConfig):
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

def display_unit_mapping_panel(config: MeasurementConfig, snowsesh: SnowflakeConnection):
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

    column_layout = [1, 2, 2, 1, 2]
    header_cols = st.columns(column_layout)
    headers = ["**Count**", "**Source Unit**", "**LQ/MED/UQ**", "**%Numeric**", "**Target Unit**"]
    for col, header in zip(header_cols, headers):
        col.write(header)

    with st.container(height=600):
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
                cols[2].write(f"{lq:.1f} / {median:.1f} / {uq:.1f}")
            else:
                cols[2].write("No numeric data")

            # numeric %
            num_percent = 0.0 if row['TOTAL_COUNT'] == 0 else 100.0
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

def main():
    st.set_page_config(page_title="Measurement Standardisation", layout="wide")
    set_font_lato()
    st.title("Measurement Standardisation")
    load_dotenv()

    if "snowsesh" not in st.session_state:
        with st.spinner("Connecting to Snowflake..."):
            st.session_state.snowsesh = connect_to_snowflake()

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
                created, updated, new_units = update_all_measurement_configs(st.session_state.snowsesh)

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

    # create tabs
    unit_std_tab, cleaning_tab, feature_tab = st.tabs([
        "Unit Mapping",
        "Measurement Cleaning",
        "Feature Generation"
    ])

    # TAB 1: UNIT MAPPING
    with unit_std_tab:
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
                    display_unit_mapping_panel(config, st.session_state.snowsesh)
                else:
                    st.info("Please add standard units first to enable unit mapping.")

if __name__ == "__main__":
    main()