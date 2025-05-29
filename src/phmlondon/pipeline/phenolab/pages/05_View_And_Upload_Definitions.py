import datetime
import glob
import os
import subprocess
import sys

import pandas as pd
import streamlit as st
from dotenv import load_dotenv
from datetime import datetime

from phmlondon.definition import Definition
from phmlondon.snow_utils import SnowflakeConnection
from utils.style_utils import set_font_lato

load_dotenv()

def get_definitions_list():
    """
    Get list of all definition jsons from the definitions directory
    """
    definitions_dir = "data/definitions"
    if not os.path.exists(definitions_dir):
        return []

    definition_files = glob.glob(os.path.join(definitions_dir, "*.json"))
    return [os.path.basename(f) for f in definition_files]


def display_definition_content(definition_file):
    """
    Display content from a selected definition
    """
    try:
        file_path = os.path.join("data/definitions", definition_file)
        definition = Definition.from_json(file_path)

        # definition info
        st.subheader(f"Definition: {definition.definition_name}")
        st.caption(f"ID: {definition.definition_id} | Version: {definition.definition_version}")
        st.caption(f"Source: {definition.definition_source}")

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
        raise e


def upload_definitions_to_snowflake():
    """
    Unions all and uploads to Snowflake Definition Library
    """
    definition_files = get_definitions_list()
    if not definition_files:
        st.error("No definition files found to upload")
        return

    # connect
    with st.spinner("Connecting to Snowflake..."):
        try:
            snowsesh = SnowflakeConnection()
            snowsesh.use_database("INTELLIGENCE_DEV")
            snowsesh.use_schema("AI_CENTRE_DEFINITION_LIBRARY")

            st.success("Connected to Snowflake")
        except Exception as e:
            st.error(f"Failed to connect to Snowflake: {e}")
            return

    # upload_time = datetime.datetime.now() 
    all_rows = pd.DataFrame()
    definitions_to_remove = {}
    definitions_to_add = []

    with st.spinner(f"Processing {len(definition_files)} definition files..."):
        for def_file in definition_files:
            try:
                file_path = os.path.join("data/definitions", def_file)
                definition = Definition.from_json(file_path)
                
                query = f"""
                SELECT DEFINITION_ID, DEFINITION_NAME, VERSION_DATETIME 
                FROM AIC_DEFINITIONS 
                WHERE DEFINITION_ID = '{definition.definition_id}'
                """
                existing_definition = snowsesh.execute_query_to_df(query)

                if not existing_definition.empty:
                    max_version_in_db = existing_definition["VERSION_DATETIME"].max()
                    current_version = definition.version_datetime

                    print(definition.definition_name)
                    print(current_version)
                    print(max_version_in_db)

                    if current_version == max_version_in_db:
                        st.info(f"Skipping {def_file} as it already exists in the database")
                        continue

                    if current_version < max_version_in_db:
                        st.info(f"Skipping {def_file} as a newer version {max_version_in_db} exists in the database")
                        continue

                    # otherwise, we have a newer version and should record that we want to delete the old one
                    definitions_to_remove[definition.definition_id] = [definition.definition_name, current_version]

                    
                # for codelist in definition.codelists:
                #     for code in codelist.codes:
                #         row = {
                #             "CODE": code.code,
                #             "CODE_DESCRIPTION": code.code_description,
                #             "VOCABULARY": code.code_vocabulary,
                #             "CODELIST_ID": codelist.codelist_id,
                #             "CODELIST_NAME": codelist.codelist_name,
                #             "CODELIST_VERSION": codelist.codelist_version,
                #             "DEFINITION_ID": definition.definition_id,
                #             "DEFINITION_NAME": definition.definition_name,
                #             "DEFINITION_VERSION": definition.definition_version,
                #             "DEFINITION_SOURCE": definition.definition_source,
                #             "VERSION_DATETIME": definition.version_datetime,
                #             "UPLOADED_DATETIME": definition.uploaded_datetime,
                #         }
                
                # put the current timestamp as the uploaded datetime
                definition.uploaded_datetime = datetime.now()
                
                all_rows = pd.concat([all_rows, definition.to_dataframe()])
                definitions_to_add.append(definition.definition_name)

            except Exception as e:
                st.error(f"Error processing {def_file}: {e}")
                raise e

    # upload
    if not all_rows.empty:
        with st.spinner(f"Uploading {len(all_rows)} rows to Snowflake..."):
            try:
                df = pd.DataFrame(all_rows)
                df.columns = df.columns.str.upper()
                # st.write(df)
                snowsesh.load_dataframe_to_table(df=df, table_name="AIC_DEFINITIONS", mode="append")
                st.success(f"Successfully uploaded new definitions {definitions_to_add} to the AIC definition library")

                # delete old versions
                for id, [name, current_version] in definitions_to_remove.items():
                    snowsesh.session.sql(
                            f"""DELETE FROM AIC_DEFINITIONS WHERE DEFINITION_ID = '{id}' AND 
                            VERSION_DATETIME != CAST('{current_version}' AS TIMESTAMP)"""
                        ).collect()
                    st.info(f"Deleted old version(s) of {name}")

                # run update.py script to refresh DEFINITIONSTORE
                with st.spinner("Updating DEFINITIONSTORE..."):
                    try:
                        current_dir = os.path.dirname(os.path.abspath(__file__))
                        update_script_path = os.path.normpath(
                            os.path.join(current_dir, "../../definition_library/update.py")
                        )

                        if not os.path.exists(update_script_path):
                            raise FileNotFoundError(f"Update script not found at {update_script_path}")

                        result = subprocess.run(
                            [sys.executable, update_script_path], capture_output=True, text=True, check=True
                        )
                        st.success("Definition store updated successfully")
                    except subprocess.CalledProcessError as e:
                        st.error(f"Error updating definition store: {e.stderr}")
                    except Exception as e:
                        st.error(f"Error executing update script: {str(e)}")

            except Exception as e:
                st.error(f"Error uploading to Snowflake: {e}")
                raise e
    else:
        st.warning("No data to upload")


def main():
    st.set_page_config(page_title="View and upload custom definitions", layout="wide")
    set_font_lato()
    st.title("View and upload custom definitions")
    st.markdown("This page will upload all definitions to `AI_CENTRE_DEFINITION_LIBRARY.AIC_DEFINITIONS`")

    _, b, _ = st.columns(3)
    [maincol] = st.columns(1)

    # Row 2: upload functionality
    definition_count = len(get_definitions_list())
    with b:
        st.text(" ")
        if definition_count > 0:
            if st.button(f"Upload new definitions to Snowflake"):
                with maincol:
                    upload_definitions_to_snowflake()
        else:
            st.warning("No definitions available to upload")

    st.markdown("---")

    # Row 1: defintion display
    col1, col2 = st.columns([1, 1.5])

    with col1:
        st.subheader("Available Definitions")
        definition_files = get_definitions_list()

        if not definition_files:
            st.info("No definition files found. Create some definitions first.")
        else:
            selected_definition = st.selectbox("Select a definition to view", options=definition_files)

    with col2:
        if "selected_definition" in locals() and selected_definition:
            display_definition_content(selected_definition)
        else:
            st.info("Select a definition from the list to view its contents")


if __name__ == "__main__":
    main()