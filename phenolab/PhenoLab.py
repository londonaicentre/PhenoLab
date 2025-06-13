import glob
import os

import pandas as pd
import streamlit as st
from dotenv import load_dotenv
from utils.database_utils import get_snowflake_session
from utils.style_utils import set_font_lato
from utils.definition_interaction_utils import update_aic_definitions_table

from definition_library.loaders.load_hdruk import retrieve_hdruk_definitions_and_add_to_snowflake
from definition_library.loaders.load_open_codelists import retrieve_open_codelists_definitions_and_add_to_snowflake
from definition_library.loaders.load_bnf_to_snomed import retrieve_bnf_definitions_and_add_to_snowflake

# # PhenoLab.py

# Main entry point for PhenoLab application.
# Creates the single Snowflake connection used throughout the app.
# Pre-Loads the most recent vocabulary (if available)

st.set_page_config(page_title="PhenoLab", layout="wide", initial_sidebar_state="expanded")
set_font_lato()
load_dotenv(override=True)

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

        return True, "Vocabulary loaded in session"

    except Exception as e:
        return False, f"Error loading vocabulary: {e}"

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

## PAGE DISPLAY
st.title("PhenoLab: Clinical Definitions and Phenotype Creator")

# st.write(st.__version__)

# display snowflake status
st.markdown(f"Connection Status: `{connection_status}`")

# display vocab status
if vocab_loaded:
    st.markdown(f"Vocabulary Status: `{vocab_message}`")
else:
    st.markdown(f"Vocabulary Status: `{vocab_message}`")

# Populate the definition tables - once per session only
# 1. AI Centre
if 'uploaded_aic_definitions' not in st.session_state:
    with st.spinner("Loading AI Centre definitions...", show_time=True):
        update_aic_definitions_table(session, database="INTELLIGENCE_DEV", schema="AI_CENTRE_DEFINITION_LIBRARY", 
            verbose=False)
        st.session_state['uploaded_aic_definitions'] = True

# 2. HDRUK
if 'uploaded_hdruk_defs' not in st.session_state:
    with st.spinner("Retrieving HDRUK definitions...", show_time=True): 
        retrieve_hdruk_definitions_and_add_to_snowflake(session, 
        database="INTELLIGENCE_DEV", schema="AI_CENTRE_DEFINITION_LIBRARY")
        st.session_state['uploaded_hdruk_defs'] = True
    
# 3. NHS GP refsets
if 'uploaded_nhs_gp_defs' not in st.session_state:
    pass # need to sort this out once I have access or else we download as static file
    st.session_state['uploaded_nhs_gp_defs'] = True

# 4. Open Codelists
if 'uploaded_open_codelists_defs' not in st.session_state:
    with st.spinner("Retrieving Open Codelists definitions...", show_time=True): 
        retrieve_open_codelists_definitions_and_add_to_snowflake(session, 
        database="INTELLIGENCE_DEV", schema="AI_CENTRE_DEFINITION_LIBRARY")
        st.session_state['uploaded_open_codelists_defs'] = True
    
# 5. BNF definitions
if 'uploaded_bnf_defs' not in st.session_state:
    with st.spinner("Retrieving BNF definitions...", show_time=True): 
        retrieve_bnf_definitions_and_add_to_snowflake(session, 
        database="INTELLIGENCE_DEV", schema="AI_CENTRE_DEFINITION_LIBRARY")
        st.session_state['uploaded_bnf_defs'] = True

required_checks = [
    'uploaded_aic_definitions',
    'uploaded_hdruk_defs',
    'uploaded_nhs_gp_defs',
    'uploaded_open_codelists_defs',
    'uploaded_bnf_defs'
]

if all(key in st.session_state for key in required_checks):
    st.markdown('Database status: `Database checked`')
else:
    st.markdown('Databse status:')
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

