import glob
import os

import pandas as pd
import streamlit as st
from dotenv import load_dotenv
from utils.database_utils import get_snowflake_session
from utils.style_utils import set_font_lato


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

st.write(st.__version__)

# display snowflake status
st.markdown(f"Connection Status: `{connection_status}`")

# display vocab status
if vocab_loaded:
    st.markdown(f"Vocabulary Status: `{vocab_message}`")
else:
    st.markdown(f"Vocabulary Status: `{vocab_message}`")

st.markdown("---")

st.markdown("""
PhenoLab helps manage:
1. **Medical Definitions**: Collections of clinical codes from different vocabularies (SNOMED, ICD10, etc.) that represent specific medical concepts
2. **Measurements**: Using definitions of measurements with numerical values, mapping and standardising units and conversions
3. **Phenotypes**: Patient-centric clinical labels based on logical operations applied to these definitions)
""")

# with st.expander("Snowflake Connection Setup"):
#     st.markdown("""
#     Required environment variables `.env`:
#     - `SNOWFLAKE_SERVER`: Snowflake account identifier
#     - `SNOWFLAKE_USER`: Snowflake username
#     - `SNOWFLAKE_USERGROUP`: Snowflake role
#     """)

st.markdown("---")
st.markdown("2025 London AI Centre & OneLondon")