import streamlit as st
import pandas as pd
from scipy import stats
import numpy as np
import altair as alt
from dotenv import load_dotenv
from phmlondon.snow_utils import SnowflakeConnection
import json
import folium
from streamlit_folium import folium_static
import geopandas as gpd
import plotly.graph_objects as go

# This code when ran with streamlit run, will open a prototype dashboard that looks at lipid lowering medication prescriptions in those who are eligible for lipid lowring meds.
# So far the code looks at 2 NICE guidance criteria, High Qrisk over 10 and Type 1 Diabetes (T1DM)
# Will be updating dashboard with Proportion Days Covered (PDC) calculations and menu to choos from BNF drug classes.
# code also needs a lot of tidying, cleaning and commenting!

load_dotenv()

if 'snowsesh' not in st.session_state:
        st.session_state.snowsesh = SnowflakeConnection()
        st.session_state.snowsesh.use_database("INTELLIGENCE_DEV")
        st.session_state.snowsesh.use_schema("AI_CENTRE_FEATURE_STORE")

snowsesh = st.session_state.snowsesh

# Title and description
st.title("Lipid Lowering Meds - Prototype")
st.write(
    """This app explores the NICE guidance for prescribing lipid-lowering medications for primary prevention of CVD.
    Currently, eligibility is based on two simple refsets to determine eligibility for primary prevention prescription and limited to 1000 patients.
    """
)

# Execute the SQL query to get the data from the view
query = """
SELECT * 
FROM intelligence_dev.ai_centre_dev.lipid_meds_explore
LIMIT 1000
"""

# Fetch the data into a Snowpark DataFrame
df = snowsesh.execute_query_to_df(query)

# Convert all column names to lowercase
df.columns = df.columns.str.lower()
# display interactive df
st.subheader("Patients eligible for Lipid lowering drugs based on 2 criteria")
st.dataframe(df)

# Count the number of NaN values in the 'different_drugs' column
nan_count = df['different_drugs'].isna().sum()

st.write('missing lipid prescriptions:' ,nan_count, 'patients,', nan_count/len(df)*100 ,'%')

missing_prescriptions = df[df['different_drugs'].isna()]
missing_by_eligibility =  missing_prescriptions.groupby('eligibility_reason').size().reset_index(name='missing_count')
total_by_eligibility = df.groupby('eligibility_reason').size().reset_index(name='total_count')
missing_by_eligibility = missing_by_eligibility.merge(total_by_eligibility, on='eligibility_reason')
missing_by_eligibility['missing_percentage'] = (missing_by_eligibility['missing_count'] / missing_by_eligibility['total_count']) * 100
# st.dataframe(missing_by_eligibility)

bar_data_missing = missing_by_eligibility.set_index('eligibility_reason')['missing_percentage']
st.subheader("% Patients without lipid prescrition by eligibility criteria")
# Display the bar chart
st.bar_chart(bar_data_missing)


# lets look at number of different drugs for those who are prescribed LLP
st.subheader("How many different drugs prescribed?")
lld_prescribed = df[~df['different_drugs'].isna()]
diff_drugs = lld_prescribed['different_drugs'].value_counts()
st.bar_chart(diff_drugs)

# let look at first drug 
st.subheader("First drug prescribed")

drug_mapping = {
    'Atorvastatin': 'Atorvastatin',
    'Simvastatin': 'Simvastatin',
    'Rosuvastatin': 'Rosuvastatin',
    'Ezetimibe': 'Ezetimibe'
}

# Function to categorize drug names individually
def categorise_drug(drug_name):
    for drug in drug_mapping:
        if drug.lower() in drug_name.lower():
            return drug_mapping[drug]  # Return the exact drug name
    return 'Other'  # If no match, categorise as 'Other'

# Apply the categorisation function to the 'drug_name' column
lld_prescribed['drug_group'] = lld_prescribed['drug_name'].apply(categorise_drug)

first_drug = lld_prescribed['drug_group'].value_counts()
st.bar_chart(first_drug)

# time between switch?


# Ethnicity and Gender Breakdown
st.subheader("Ethnicity and Gender Breakdown")
# Plot Gender distribution
gender_data = df['gender'].value_counts()

# Display the Gender distribution using st.bar_chart
st.subheader("Gender Distribution")
st.bar_chart(gender_data)

# Plot Ethnicity distribution
ethnicity_data = df['ethnicity'].value_counts()

# Display the Ethnicity distribution using st.bar_chart
st.subheader("Ethnicity Distribution")
st.bar_chart(ethnicity_data)