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
    LIMIT 10000000
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

    #For now bin patients where DOB before 1900
    nel_index = nel_index.loc[nel_index.DATE_OF_BIRTH > datetime(1900, 1,1), :]
    nel_index = nel_index.reset_index().iloc[:, 1:]

    def years_between(later_years: pd.Series, earlier_years: pd.Series) -> pd.Series:
        """Function to calculate a time difference in years
        later_years: pd.Series of the later dates (in datetime)
        earlier_years: pd.Series of the earlier dates (in datetime)
        Returns another series of type float in years
        """
        #Some sensible error messages for type checking - pd.series
        if not isinstance(later_years, pd.Series) and not (isinstance, earlier_years, pd.Series):
            raise TypeError('Both inputs must be series')
        
        #Errors for datetime
        later_years_dt = later_years.apply(lambda x: isinstance(x, datetime)).all()
        earlier_years_dt = earlier_years.apply(lambda x: isinstance(x, datetime)).all()
        if not earlier_years_dt and not later_years_dt:
            raise TypeError('Both series must only contain datetimes')
        
        years_inbetween = later_years - earlier_years
        return years_inbetween.dt.days/365.25
    
    #Fix issue with some registration dates being before birth
    registered_before_birth = nel_index.REGISTRATION_START_DATE < nel_index.DATE_OF_BIRTH
    nel_index.loc[registered_before_birth, 'REGISTRATION_START_DATE'] = nel_index.DATE_OF_BIRTH[registered_before_birth]

    #Check if currently registered - registration end in future if still registered
    registration_now = nel_index.END_OF_MONTH - nel_index.REGISTRATION_END_DATE
    registration_death = nel_index.DATE_OF_DEATH - nel_index.REGISTRATION_END_DATE
    nel_index['currently_registered'] = registration_now.dt.days < 0

    #Now mark patients if registered at death - registration end date ends within 2 months of death (arbitrarily)
    nel_index.loc[registration_death.dt.days <= 60, 'currently_registered'] = True 

    #Get ages
    nel_index['age'] = np.nan

    died = ~nel_index.DATE_OF_DEATH.isna()
    nel_index.loc[died, 'age'] = years_between(nel_index.DATE_OF_DEATH[died], nel_index.DATE_OF_BIRTH[died])
    nel_index.loc[~died, 'age'] = years_between(nel_index.END_OF_MONTH[~died], nel_index.DATE_OF_BIRTH[~died])

    #nel_index.loc[nel_index.age > 125, ['DATE_OF_BIRTH', 'REGISTRATION_START_DATE', 'REGISTRATION_END_DATE', 'END_OF_MONTH']]
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

   #Can either plot ages at death or do some sort of standardised mortality rate? Prob makes sense to do both

    #Plot time from registration to death
    newcol1, newcol2 = st.columns(2)
    with newcol1:
        died_bins = st.slider('Bins (Deaths)', 0, 100, 20)
        fig_died = px.histogram(nel_index.loc[died, :], 
                                x = 'age', 
                                color = 'currently_registered', 
                                nbins = died_bins, #Slider from above
                                category_orders=dict(currently_registered = [False, True])
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

    ### This is quite low. Would be interesting to do a mortality rate for each age bracket
    age_brackets = np.array([(i+1)*5 for i in range(25)])
    
    #Work out the time spent in each age bracket and then number of deaths
    #First annoyingly need to assume that age at registration where not filled is birth and age at deregistration now/ death
    nel_index.loc[nel_index.REGISTRATION_START_DATE.isna(), 'REGISTRATION_START_DATE'] = nel_index.DATE_OF_BIRTH
    nel_index.loc[nel_index.REGISTRATION_END_DATE.isna(), 'REGISTRATION_END_DATE'] = nel_index.END_OF_MONTH

    #Make age at registration and at deregistration
    nel_index['age_registration'] = years_between(nel_index.REGISTRATION_START_DATE, nel_index.DATE_OF_BIRTH)
    nel_index['age_deregistration'] = years_between(nel_index.REGISTRATION_END_DATE, nel_index.DATE_OF_BIRTH)
    nel_index.loc[died_registered, 'age_deregistration'] = nel_index.age[died_registered]
    nel_index.loc[alive_registered, 'age_deregistration'] = nel_index.age[alive_registered]

    #Now get times in each age bracket
    def years_between_brackets(start_age: float, end_age: float, brackets: np.array) -> np.array:
        """Function to return the years spent in each bracket"""

        #Enforce rules
        bracket_size = np.unique(np.diff(brackets))
        if not len(bracket_size) == 1:
            raise ValueError('Brackets must be equal distances apart')
        
        #Get start and end
        start_bracket = brackets[brackets > start_age][0]
        end_bracket = brackets[brackets > end_age][0]

        #Set up some storage, get start and end
        age_in_brackets = np.zeros(len(brackets))
        age_in_brackets[brackets == start_bracket] = start_bracket - start_age
        age_in_brackets[brackets == end_bracket] = end_age - (end_bracket - bracket_size[0])

        #Fill in the gaps
        middle_brackets = [i for i in range(start_bracket + bracket_size[0], end_bracket, bracket_size[0])]
        age_in_brackets[np.isin(brackets, middle_brackets)] = 5

        return(age_in_brackets)
    
    @st.cache_data
    def explode_rows(sheet: pd.DataFrame)-> pd.DataFrame:
        """Function to take vectorised output from applied years_between_brackets 
        and convert to a dataframe"""
        return sheet.apply(lambda x: x.explode(1), axis = 1)
    
    @st.cache_data
    def return_brackets_df(sheet: pd.DataFrame, start_age: str, end_age: str, brackets: np.array, from_birth = False) -> pd.DataFrame:
        """Function to put all of this together"""
        if not from_birth:
            new_sheet = sheet.apply(lambda x: years_between_brackets(x[start_age], x[end_age], brackets), axis = 1)
        else: 
            new_sheet = sheet.apply(lambda x: years_between_brackets(0, x[end_age], brackets), axis = 1)
        new_sheet = pd.DataFrame(new_sheet)
        return explode_rows(new_sheet)

    registered_age_df = return_brackets_df(nel_index, 'age_registration', 'age_deregistration', age_brackets)
    preregistered_age_df = return_brackets_df(nel_index, 'birth', 'age_registration', age_brackets, from_birth=True)
    postregistered_age_df = return_brackets_df(nel_index, 'age_deregistration', 'age', age_brackets)

    #Add up the times
    registered_age_bracket_sums = registered_age_df.sum(axis = 0)
    prereg_age_bracket_sums = preregistered_age_df.sum(axis = 0)
    postreg_age_bracket_sums =  postregistered_age_df.sum(axis = 0)
    unreg_age_bracket_sums = postreg_age_bracket_sums + prereg_age_bracket_sums

    #Now get mortality by age
    registered_death_brackets = pd.cut(nel_index.loc[died_registered, 'age'], age_brackets - 5).value_counts()
    registered_death_brackets_np = registered_death_brackets.sort_index().to_numpy()
    unregistered_death_brackets = pd.cut(nel_index.loc[died_deregistered, 'age'], age_brackets - 5).value_counts()
    unregistered_death_brackets_np = unregistered_death_brackets.sort_index().to_numpy()
    
    #Now calculate the standardised mortality rate
    age_stratified_mortality_registered = np.append(registered_death_brackets_np, 0)/registered_age_bracket_sums
    age_strat_mort_reg_df = pd.DataFrame({'age': age_brackets,
                                          'standardised_mortality': age_stratified_mortality_registered
                                          })
    age_stratified_mortality_unregistered = np.append(unregistered_death_brackets_np, 0)/unreg_age_bracket_sums
    age_strat_mort_unreg_df = pd.DataFrame({'age': age_brackets,
                                          'standardised_mortality': age_stratified_mortality_unregistered
                                          })

    registered_barplot = px.bar(age_strat_mort_reg_df,x= 'age', y = 'standardised_mortality')
    unregistered_barplot = px.bar(age_strat_mort_unreg_df,x= 'age', y = 'standardised_mortality')

    regcol1, regcol2 = st.columns(2)
    with regcol1:
        st.markdown('Age stratified mortality in registered patients')
        st.plotly_chart(registered_barplot)

    with regcol2:
        st.markdown('Age stratified mortality in unregistered patients')
        st.plotly_chart(unregistered_barplot, color = 'red')

if __name__ == "__main__":
    main()