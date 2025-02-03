"""
Script to run a streamlit app which visualisation of some data quality bits
"""

import streamlit as st
import pandas as pd
from dotenv import load_dotenv
import matplotlib.pyplot as plt
from datetime import datetime
import numpy as np
from phmlondon.snow_utils import SnowflakeConnection

def main():
    load_dotenv()

    if 'snowsesh' not in st.session_state:
        st.session_state.snowsesh = SnowflakeConnection()
        st.session_state.snowsesh.use_database("INTELLIGENCE_DEV")
        st.session_state.snowsesh.use_schema("AI_CENTRE_PHENOTYPE_LIBRARY")

    snowsesh = st.session_state.snowsesh
    st.title("Initial Data Quality Explorer")
    st.write(
        """
        Explore how the data quality is within the phenotype store, starting with death/censoring
        """
    )

    # Pull NEL master index dataframe
    source_query = """
    select *
    from INTELLIGENCE_DEV.AI_CENTRE_FEATURE_STORE.PERSON_NEL_MASTER_INDEX
    LIMIT 1000000
    """
    nel_index = snowsesh.execute_query_to_df(source_query)

    #Convert some columns to datetime
    dt_columns = ['START_OF_MONTH', 
                  'END_OF_MONTH', 
                  'REGISTRATION_START_DATE', 
                  'REGISTRATION_END_DATE', 
                  'LATEST_SMOKING_STATUS_DATE', 
                  'LATEST_BMI_DATE', 
                  'DATE_OF_DEATH', 
                  'DATE_OF_BIRTH'
                  ]
    nel_index[dt_columns] = nel_index[dt_columns].apply(pd.to_datetime, errors='coerce')
    st.dataframe(nel_index.head())

    #Check if currently registered - registration end in future if still registered
    registration_now = datetime.now() - nel_index.REGISTRATION_END_DATE
    registration_death = nel_index.DATE_OF_DEATH - nel_index.REGISTRATION_END_DATE
    nel_index['currently_registered'] = registration_now.dt.days < 0
    nel_index.loc[registration_death.dt.days < 0, 'currently_registered'] < 0 
    
    #Some interim plotting
    fig, ax = plt.subplots(1,1)
    registration_death[~registration_death.isna()].dt.days.hist(ax = ax)
    st.pyplot(fig)

    #Or if registered at death
    nel_index.head()
    
    #Get ages
    nel_index['age'] = np.nan
    died = nel_index.PATIENT_STATUS == 'DEATH'
    death_ages = nel_index.DATE_OF_DEATH[died] - nel_index.DATE_OF_BIRTH[died]
    nel_index.loc[died, 'age'] = death_ages.dt.days/365.25

    survived_ages = nel_index.DATE_OF_DEATH[~died] - nel_index.DATE_OF_BIRTH[~died]
    nel_index.loc[~died, 'age'] = survived_ages.dt.days/365.25

    #st.pyplot(nel_index.loc[nel_index_currently_registered == True, ''])
    
if __name__ == "__main__":
    main()