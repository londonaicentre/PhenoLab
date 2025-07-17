import streamlit as st

from utils.database_utils import get_snowflake_session
from utils.style_utils import set_font_lato
from utils.config_utils import load_config, preload_vocabulary

# # PhenoLab.py

# Main entry point for PhenoLab application.
# Creates the single Snowflake connection used throughout the app.
# Pre-Loads the most recent vocabulary (if available)

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
st.session_state.session = get_snowflake_session()
try:
    st.session_state.session.sql("SELECT 1").collect()
    connection_status = "Connected to Snowflake"
except Exception as e:
    connection_status = f"Connection failed: {e}"

# Load configuration file
st.session_state.config = load_config()
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
    if st.session_state.config["local_development"]:
        addendum_str = "+ `Local Development Mode`"
    else:
        addendum_str = ""
    st.markdown(f"Configuration: `{st.session_state.config['icb_name']}`" + addendum_str)

    st.markdown('Database status: `Definitions loaded during deployment`')

st.markdown("---")

st.markdown("""
PhenoLab helps manage:
1. **Medical Definitions**: Collections of clinical codes from different vocabularies (SNOMED, ICD10, etc.) that 
    represent specific medical concepts
2. **Measurements**: Use definitions of codes with numerical values to map and standardising units and conversions
3. **Phenotypes**: Patient-centric clinical labels based on logical operations applied to these definitions
""")

st.markdown("---")
col3, col4 = st.columns(2)
with col3:
    st.markdown("2025 London AI Centre & OneLondon")
with col4:
    st.write("Running streamlit version:", st.__version__)
if st.session_state.config["deploy_env"] == "dev":
    st.markdown("**Development Mode**: `ON` - this is a development version of PhenoLab, not for production use")

