"""
Script to run a streamlit app which visualisation of some data quality bits
"""

import streamlit as st
import pandas as pd
from dotenv import load_dotenv
import matplotlib.pyplot as plt
import plotly.express as px
from plotly.subplots import make_subplots
import plotly.graph_objects as go
from datetime import datetime
import numpy as np
from phmlondon.snow_utils import SnowflakeConnection

def main():

    ## Set up the session ##
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


    ## Pull the data (NEL Person master index) ##
    @st.cache_data
    def pull_df(query, _session):
        #Cache the dataframe when it is pulled
        return _session.execute_query_to_df(query)
    
    source_query = """
    select *
    from INTELLIGENCE_DEV.AI_CENTRE_FEATURE_STORE.PERSON_NEL_MASTER_INDEX
    """
    nel_index = pull_df(source_query, snowsesh)



    ## Initial data processing and some initial plotting ##
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

    #Order by start of month, then drop duplicate patients
    nel_index = nel_index.sort_values('END_OF_MONTH', ascending = False)
    nel_index = nel_index[~nel_index.PERSON_ID.duplicated()]
    
    #Fix issue with some registration dates being before birth
    registered_before_birth = nel_index.REGISTRATION_START_DATE < nel_index.DATE_OF_BIRTH
    nel_index.loc[registered_before_birth, 'REGISTRATION_START_DATE'] = nel_index.DATE_OF_BIRTH[registered_before_birth]

    #Check if currently registered - registration end in future if still registered
    registration_now = nel_index.END_OF_MONTH - nel_index.REGISTRATION_END_DATE
    registration_death = nel_index.DATE_OF_DEATH - nel_index.REGISTRATION_END_DATE
    nel_index['currently_registered'] = registration_now.dt.days < 0

    #Now mark patients if registered at death - registration end date ends within 2 months of death (arbitrarily)
    nel_index.loc[registration_death.dt.days <= 60, 'currently_registered'] = True 
    
    #First validation plots
    st.markdown('## Plotting time from registration to death or other')
    col1, col2 = st.columns(2)
    with col1:
        xlimits = st.slider('Select x-limits', -75, 75, (-75, 75))
    with col2:
        nbins = st.slider('Number of bins', 1, 150, 50)

    #Interim plotting
    fig = make_subplots(rows = 1, cols = 2, subplot_titles=('Years from registration to death', 
                                                            'Years from end of registration to now'))

    #Plot time from registration to death
    registration_death = registration_death.dt.days/365.25
    hist1 = go.Histogram(x = registration_death[~registration_death.isna()], 
                         nbinsx=nbins, 
                         name = 'death')
    fig.append_trace(hist1, 1,1)

    regnow = registration_now.dt.days/365.25
    hist2 = go.Histogram(x= regnow, 
                         nbinsx=nbins, 
                         name = 'registration')
    fig.append_trace(hist2, 1, 2)
    fig.update_xaxes(range = xlimits)

    st.plotly_chart(fig)

    #Get ages
    nel_index['age'] = np.nan
    died = ~nel_index.DATE_OF_DEATH.isna()
    #died = nel_index.PATIENT_STATUS == 'DEATH'
    death_ages = nel_index.DATE_OF_DEATH[died] - nel_index.DATE_OF_BIRTH[died]
    nel_index.loc[died, 'age'] = death_ages.dt.days/365.25

    survived_ages = nel_index.END_OF_MONTH[~died] - nel_index.DATE_OF_BIRTH[~died]
    nel_index.loc[~died, 'age'] = survived_ages.dt.days/365.25


    #Can either plot ages at death or do some sort of standardised mortality rate? Prob makes sense to do both

    #Plot time from registration to death
    newcol1, newcol2 = st.columns(2)
    with newcol1:
        died_bins = st.slider('Bins (Deaths)', 0, 100, 20)
        fig_died = px.histogram(nel_index.loc[died, :], 
                                x = 'age', 
                                color = 'currently_registered', 
                                nbins = died_bins, #Slider from above
                                category_orders=dict(currently_registered = [True, False])
                                )
        st.plotly_chart(fig_died)

    with newcol2:
        alive_bins = st.slider('Bins (Alive)', 0, 100, 20)
        fig_alive = px.histogram(nel_index.loc[~died, :], 
                                 x = 'age', 
                                 color = 'currently_registered', 
                                 nbins = alive_bins, 
                                 category_orders=dict(currently_registered = [False, True]))
        st.plotly_chart(fig_alive)

    #Now work out a standardised mortality rate - risk of immortal time bias? - paused here
    nel_index['registered_years'] = np.nan
    
    #This is the default (for deregistered patients)
    nel_index.registered_years = nel_index.REGISTRATION_END_DATE - nel_index.REGISTRATION_START_DATE

    #Special cases
    died_registered = np.intersect1d(np.where(died), np.where(nel_index.currently_registered))
    nel_index.loc[died_registered, 'registered_years'] = nel_index.DATE_OF_DEATH[died_registered] - nel_index.REGISTRATION_START_DATE[died_registered]

    alive_registered = np.intersect1d(np.where(~died), np.where(nel_index.currently_registered))
    nel_index.loc[alive_registered, 'registered_years'] = nel_index.END_OF_MONTH[alive_registered] - nel_index.REGISTRATION_START_DATE[alive_registered]

    nel_index.registered_years = nel_index.registered_years.dt.days/365.25

    #Now for the non-registered years
    nel_index['deregistered_years'] = nel_index.age - nel_index.registered_years

    died_deregistered = np.intersect1d(np.where(died), np.where(~nel_index.currently_registered))
    standardised_mortality = {'registered': died_registered.shape[0]/nel_index.registered_years.sum(), 
                              'unregistered': died_deregistered.shape[0]/nel_index.deregistered_years.sum()}
    
    sm_df = pd.DataFrame(standardised_mortality, index = [0])
    sm_df = sm_df.transpose().reset_index()
    sm_df.columns = ['registration', 'standardised_mortality']
    barplot = px.bar(sm_df, x= 'registration', y = 'standardised_mortality')

    st.markdown('### Calculate standardised mortality')
    st.plotly_chart(barplot)

if __name__ == "__main__":
    main()