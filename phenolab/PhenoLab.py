import glob
import os

import pandas as pd
import streamlit as st

from utils.database_utils import get_snowflake_session
from utils.style_utils import set_font_lato
from utils.definition_interaction_utils import update_aic_definitions_table
from utils.config_utils import load_config
from definition_library.loaders.load_hdruk import retrieve_hdruk_definitions_and_add_to_snowflake
from definition_library.loaders.load_open_codelists import retrieve_open_codelists_definitions_and_add_to_snowflake
from definition_library.loaders.load_bnf_to_snomed import retrieve_bnf_definitions_and_add_to_snowflake
from definition_library.loaders.create_tables import create_definition_table

# # PhenoLab.py

# Main entry point for PhenoLab application.
# Creates the single Snowflake connection used throughout the app.
# Pre-Loads the most recent vocabulary (if available)

def preload_vocabulary():
    """
    Preload the most recent vocabulary file if available.
    """
    try:
        vocab_dir = "data/vocab"
        if not os.path.exists(vocab_dir):
            return False, "Vocabulary directory not found"

        vocab_files = glob.glob(os.path.join(vocab_dir, "vocab_*.parquet"))

        if not vocab_files:
            return False, "No vocabulary files found. Please generate a new vocabulary."

        # sort by filename (yyyy-mm-dd)
        most_recent_file = sorted(vocab_files)[-1]

        vocab_df = pd.read_parquet(most_recent_file)

        st.session_state.codes = vocab_df

        return True, "Vocabulary loaded"

    except Exception as e:
        return False, f"Error loading vocabulary: {e}"

st.set_page_config(page_title="PhenoLab", layout="wide", initial_sidebar_state="expanded")
set_font_lato()

# vocabulary session state
if "codes" not in st.session_state:
    st.session_state.codes = None
    vocab_loaded, vocab_message = preload_vocabulary()
else:
    vocab_loaded = st.session_state.codes is not None
    vocab_message = "Vocabulary loaded in session"

# initialise snowflake connection
session = get_snowflake_session()
try:
    session.sql("SELECT 1").collect()
    connection_status = "Connected to Snowflake"
except Exception as e:
    connection_status = f"Connection failed: {e}"

# Load configuration file
st.session_state.config = load_config(session)
print(st.session_state.config)

## PAGE DISPLAY
st.title("PhenoLab: Clinical Definition and Phenotype Creator")

col1, col2 = st.columns(2)

with col1:
    # display snowflake status
    st.markdown(f"Connection Status: `{connection_status}`")

    # display vocab status
    if vocab_loaded:
        st.markdown(f"Vocabulary Status: `{vocab_message}`")
    else:
        st.markdown(f"Vocabulary Status: `{vocab_message}`")

with col2:
    st.markdown(f"Configuration: `{st.session_state.config['icb_name']}`")

    if "debug_mode" not in st.session_state.config: #internal thing to make debugging faster 
        # - add "debug_mode" to config file and don't have to load db tables each time
        
        # Populate the definition tables - once per session only
        # 1. AI Centre
        if 'uploaded_aic_definitions' not in st.session_state:
            with st.spinner("Loading AI Centre definitions...", show_time=True):
                update_aic_definitions_table(
                    session, 
                    database=st.session_state.config["definition_library"]["database"], 
                    schema=st.session_state.config["definition_library"]["schema"], 
                    verbose=False)
                st.session_state['uploaded_aic_definitions'] = True

        # 2. HDRUK
        if 'uploaded_hdruk_defs' not in st.session_state:
            with st.spinner("Retrieving HDRUK definitions...", show_time=True): 
                retrieve_hdruk_definitions_and_add_to_snowflake(session, 
                    database=st.session_state.config["definition_library"]["database"], 
                    schema=st.session_state.config["definition_library"]["schema"])
                st.session_state['uploaded_hdruk_defs'] = True
            
        # 3. NHS GP refsets
        if 'uploaded_nhs_gp_defs' not in st.session_state:
            pass # need to sort this out once I have access or else we download as static file
            st.session_state['uploaded_nhs_gp_defs'] = True

        # 4. Open Codelists
        if 'uploaded_open_codelists_defs' not in st.session_state:
            with st.spinner("Retrieving Open Codelists definitions...", show_time=True): 
                retrieve_open_codelists_definitions_and_add_to_snowflake(session, 
                    database=st.session_state.config["definition_library"]["database"], 
                    schema=st.session_state.config["definition_library"]["schema"])
                st.session_state['uploaded_open_codelists_defs'] = True
            
        # 5. BNF definitions
        if 'uploaded_bnf_defs' not in st.session_state:
            with st.spinner("Retrieving BNF definitions...", show_time=True): 
                retrieve_bnf_definitions_and_add_to_snowflake(session, 
                    database=st.session_state.config["definition_library"]["database"], 
                    schema=st.session_state.config["definition_library"]["schema"])
                st.session_state['uploaded_bnf_defs'] = True

        # 6. Table for local definitions
        if 'created_local_definitions_table' not in st.session_state:
            with st.spinner("Creating local definitions table...", show_time=True):
                create_definition_table(
                    session, 
                    database=st.session_state.config["definition_library"]["database"], 
                    schema=st.session_state.config["definition_library"]["schema"],
                    table_name="ICB_DEFINITIONS"
                )
                st.session_state['created_local_definitions_table'] = True

        required_checks = [
            'uploaded_aic_definitions',
            'uploaded_hdruk_defs',
            'uploaded_nhs_gp_defs',
            'uploaded_open_codelists_defs',
            'uploaded_bnf_defs',
            'created_local_definitions_table'
        ]

        if all(st.session_state.get(key) for key in required_checks): #checks all true
            st.markdown('Database status: `Database checked`')
        else:
            st.markdown('Database status:')
            st.warning('Missing database checks')

st.markdown("---")

st.markdown("""
PhenoLab helps manage:
1. **Medical Definitions**: Collections of clinical codes from different vocabularies (SNOMED, ICD10, etc.) that represent specific medical concepts
2. **Measurements**: Using definitions of measurements with numerical values, mapping and standardising units and conversions
3. **Phenotypes**: Patient-centric clinical labels based on logical operations applied to these definitions)
""")

st.markdown("---")
st.markdown("2025 London AI Centre & OneLondon")

st.write("Running streamlit version:", st.__version__)

