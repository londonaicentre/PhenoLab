import streamlit as st

from utils.database_utils import get_snowflake_session
from utils.style_utils import set_font_lato
from utils.config_utils import load_config

st.set_page_config(page_title="Clean Measurements", layout="wide")
set_font_lato()
if "session" not in st.session_state:
    st.session_state.session = get_snowflake_session()
if "config" not in st.session_state:
    st.session_state.config = load_config()
st.title("Clean Measurements")


st.write('Select a measurement to view available cleaning configurations or create a new one:')





# - get a list of measurements - both ICB table and AIC table (not local jsosn)
# - select a measurement
# - add units 
# - map units
# - convert units
# - out of range
# - all of this goes into config tables!
# - regenerate table from config -> checks for if any new measurements and then does so
# - some plots


# remote table:
# ---------
# definition source
# definition
# original person, measurement, unit etc
# mapped units
# converted val and converted units
# flag for out of range





#  measurement
#  config ID
#  unit 