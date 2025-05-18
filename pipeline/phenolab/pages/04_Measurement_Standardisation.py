import os
import streamlit as st
from dotenv import load_dotenv
from typing import List, Optional, Tuple
import glob

from phmlondon.definition import Definition
from phmlondon.snow_utils import SnowflakeConnection
from utils.style_utils import set_font_lato
from utils.definition_display_utils import load_definition, load_definitions_list

from utils.measurement import MeasurementConfig, load_measurement_config_from_json
from phmlondon.config import SNOWFLAKE_DATABASE, DEFINITION_LIBRARY

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

    # extract def ids for existing configs
    config_definition_ids = []
    for config_file in measurement_configs:
        try:
            config = load_measurement_config(config_file)
            if config:
                config_definition_ids.append(config.definition_id)
        except Exception as e:
            st.warning(f"Could not load configuration file {config_file}: {e}")

    # check against def ids that are measurements
    created_count = 0
    for def_file in measurement_definitions:
        try:
            file_path = os.path.join("data/definitions", def_file)
            definition = load_definition(file_path)

            if definition and definition.definition_id not in config_definition_ids:

                # create new measurement config
                config = MeasurementConfig(
                    definition_id=definition.definition_id,
                    definition_name=definition.definition_name,
                    standard_measurement_config_id=None,  # Will be auto-generated
                    standard_measurement_config_version=None,  # Will be auto-generated
                )
                config.save_to_json()
                created_count += 1

        except Exception as e:
            st.warning(f"Could not process {def_file}: {e}")

    return created_count

def display_standard_units_panel(config: MeasurementConfig):
    """
    Display panel for managing standard units
    """
    st.subheader("Configure Standard Units")

    # add new unit layout
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
            # save immediately
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
                        # immediately saves
                        config.save_to_json()
                        st.rerun()
            with col3:
                if st.button("Remove", key=f"remove_unit_{i}"):
                    config.remove_standard_unit(unit)
                    # immediately saves
                    config.save_to_json()
                    st.rerun()
    else:
        st.info("No standard units defined.")

def main():
    st.set_page_config(page_title="Measurement Standardisation", layout="wide")
    set_font_lato()
    st.title("Measurement Standardisation")

    # check and create missing measurement configs
    with st.spinner("Checking measurement configurations..."):
        created_count = create_missing_measurement_configs()
        if created_count > 0:
            st.success(f"Created {created_count} new measurement configuration{'s' if created_count > 1 else ''}")

    # create tabs
    unit_std_tab, cleaning_tab, feature_tab = st.tabs([
        "Unit Mapping",
        "Measurement Cleaning",
        "Feature Generation"
    ])

    # TAB 1: UNIT MAPPING
    with unit_std_tab:
        st.subheader("Select Measurement Config File")

        # Refresh configs list each time
        measurement_configs = load_measurement_configs_list()
        if not measurement_configs:
            st.warning("No measurement configurations found. Please check data/measurements.")
            return

        # map definition name to filename
        config_by_name = {}

        for config_file in measurement_configs:
            try:
                config = load_measurement_config(config_file)
                if config:
                    config_by_name[config.definition_name] = config_file
            except Exception as e:
                st.error(f"Error loading {config_file}: {e}")
                pass

        # make sure we have options
        if not config_by_name:
            st.warning("Could not load any valid measurement configurations.")
            return

        # selection + load
        selected_def_name = st.selectbox(
            "Select a measurement configuration",
            options=sorted(config_by_name.keys()),
            key="measurement_config_select"
        )

        if selected_def_name:
            config_filename = config_by_name[selected_def_name]
            config = load_measurement_config(config_filename)

            if config:
                st.info(f"**Selected Configuration**: {config.definition_name}")
                st.markdown("---")

                # standard units
                display_standard_units_panel(config)

                # save back
                # if st.button("Save Configuration"):
                #     filepath = config.save_to_json()
                #     st.success(f"Configuration saved to: {filepath}")

if __name__ == "__main__":
    main()