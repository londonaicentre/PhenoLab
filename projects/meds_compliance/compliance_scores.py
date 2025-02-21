import streamlit as st
from dotenv import load_dotenv
import pandas as pd
from phmlondon.snow_utils import SnowflakeConnection
import statsmodels.api as sm
import statsmodels.formula.api as smf

# This code when run with streamlit run will open a dashboard to investigate the assocaition of calculated pdc vs recorded compliance statement (good or poor)
# Still massively a work in progress

def main():
    # Load environment variables
    load_dotenv()


    if 'snowsesh' not in st.session_state:
        st.session_state.snowsesh = SnowflakeConnection()
        st.session_state.snowsesh.use_database("INTELLIGENCE_DEV")
        st.session_state.snowsesh.use_schema("AI_CENTRE_FEATURE_STORE")

    snowsesh = st.session_state.snowsesh

    # Query to fetch data
    query = """ 
    SELECT 
        overall_compliance_score,
        medication_compliance
    FROM 
        intelligence_dev.ai_centre_dev.comp_person_pdc
        limit 10000
    """
    
    # Execute query and fetch data into a pandas DataFrame
    df = snowsesh.query(query)

    st.write("### Data Preview", df)

    # Map medication_compliance to an ordinal scale
    compliance_mapping = {'good': 2, 'both': 1, 'poor': 0}
    df['medication_compliance_ordinal'] = df['medication_compliance'].map(compliance_mapping)

    # Fit the ordinal logistic regression model
    model = smf.mnlogit('medication_compliance_ordinal ~ overall_compliance_score', data=df)

    # Fit the model and display the results
    try:
        result = model.fit()
        st.write("### Regression Results", result.summary())
    except Exception as e:
        st.write(f"Error fitting the model: {str(e)}")

# Run the main function when the script is executed
if __name__ == "__main__":
    main()