"""
Streamlit app for diabetes dashboard
"""
local = True #True = running from own computer; false = running from snowflake interface

if local:
    from phmlondon.snow_utils import SnowflakeConnection, Session
    from dotenv import load_dotenv
from snowflake.snowpark import Session
from snowflake.snowpark.context import get_active_session
import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from datetime import datetime, timedelta

@st.cache_resource
def connect(local: bool) -> Session:
    if local:
        load_dotenv()
        conn = SnowflakeConnection()
        conn.use_database("INTELLIGENCE_DEV")
        conn.use_schema("AI_CENTRE_DEV")
        return conn.session
    else:
        session = get_active_session()
        return session
        

@st.cache_data
def load_data(_session: Session, table_name: str) -> pd.DataFrame:
    df = _session.table(table_name)
    df = df.to_pandas()
    df.columns = df.columns.str.lower()
    return df

st.title("NEL Diabetes Dashboard")

conn = connect(local)
df1 = load_data(conn, "PATIENTS_WITH_T2DM")
df2 = load_data(conn, "HBA1C_LEVELS_WITH_CORRECTED_UNITS")
df3 = load_data(conn, "UNDIAGNOSED_PATIENTS_WITH_T2DM")

if local:
    box1, box2 = st.columns(2, border=True, vertical_alignment="bottom")
    box3, box4 = st.columns(2, border=True, vertical_alignment="bottom")
    box5, box6 = st.columns(2, border=True, vertical_alignment="bottom")
else:
    # snowflake runs an out of date version of streamlit which doesn't have the border
    # parameter for this function :(
    box1, box2 = st.columns(2, vertical_alignment="bottom")
    box3, box4 = st.columns(2, vertical_alignment="bottom")
    box5, box6 = st.columns(2, vertical_alignment="bottom")

colours = ["#003f5c", "#444e86", "#955196", "#dd5182", "#ff6e54", "#ffa600"]

with box1:
    st.subheader('Age')

    data_column = 'age_if_patient_living'
    # data_column = st.selectbox("Select a column to plot:", df1.select_dtypes(include='number').columns, index=5)
    bins = st.slider("Number of bins:", min_value=5, max_value=100, value=10)

    fig, ax = plt.subplots()
    ax.hist(df1[data_column], bins=bins, edgecolor='white', color=colours[0])
    ax.set_xlabel(data_column)
    ax.set_ylabel("Count")

    st.pyplot(fig)

# st.write(df1)

with box2:
    st.subheader('Gender and ethnicity')
    # st.write("")
    
    plot_choice = st.segmented_control("Select which to plot:", ["Gender", "Ethnicity"], default='Ethnicity')
    if plot_choice == 'Gender':
        st.write("")

    col_name = {'Gender': 'gender_as_text', 'Ethnicity': 'ethnic_category'} 
    #or could do ethnicity_description which is more fine-grained e.g. 'bangladeshi' rather than 'south asian'

    counts = df1[col_name[plot_choice]].value_counts()

    fig2, ax2 = plt.subplots()
    ax2.bar(counts.index, counts.values, edgecolor=None, color=colours)
    # ax2.set_xlabel(plot_choice)
    ax2.set_ylabel("Count")
    ax2.tick_params(axis='x', rotation=45) 
    # ax2.set_title("Male vs Female Distribution")

    st.pyplot(fig2)

with box3:
    st.subheader('HbA1c levels')

    threshold = 48
    data = df2['result_value_cleaned_and_converted_to_mmol_per_mol'].dropna()

    bins = np.histogram_bin_edges(data, bins=50)

    fig, ax = plt.subplots()    
    n, bins, patches = ax.hist(data, bins=bins, edgecolor='white')
    for patch, bin_edge in zip(patches, bins[:-1]):
        if bin_edge>=threshold:
            patch.set_facecolor(colours[2])
        else:
            patch.set_facecolor(colours[4])
    ax.set_xlabel('HbA1c level (mmol/mol))')
    ax.set_ylabel("Count")

    st.pyplot(fig)

with box4:
    st.subheader("Undiagnosed patients")
    st.write("")
    st.write("")
    
    add_commas = lambda x: f"{x:,}"
    st.metric(label="Patients with a code indicating type 2 diabetes", value=add_commas(df1.shape[0]))
    st.write("")
    
    st.metric(label="Patients with no diabetes code but a diabetic HbA1c", value=add_commas(df3.shape[0]))
    st.write("")

with box5:
    st.subheader("Max HbA1c in the unlablled patients")
    bins = st.slider("Number of bins:", min_value=5, max_value=100, value=50)
    max_value = st.number_input("Enter max value:", min_value=0, max_value=140, value=100, step=1)
    fig, ax = plt.subplots()
    ax.hist(df3["max_hba1c"], edgecolor="white", color=colours[5], bins=bins, range=(48, max_value))
    ax.set_xlabel('HbA1c level (mmol/mol))')
    ax.set_ylabel("Count")
    st.pyplot(fig)

with box6:

    @st.cache_data
    def bin_diagnoses_by_month():
        df1["diagnosis_date"] = pd.to_datetime(df1["diagnosis_date"])
        df1["year_month"] = df1["diagnosis_date"].dt.to_period("M")
        monthly_counts = df1.groupby("year_month").size().reset_index(name="num_diagnoses")
        monthly_counts["year_month"] = monthly_counts["year_month"].dt.to_timestamp()
        return monthly_counts
    
    monthly_counts = bin_diagnoses_by_month()

    overall_start = datetime(1980, 1, 1)
    default_start = datetime(2010, 1, 1)
    today = datetime.today()
    first_day_of_month = today.replace(day=1)
    default_end = first_day_of_month - timedelta(days=1)

    st.subheader("New diagnoses by month")
    st.write("")
    start_date = st.date_input("Select start Date", value=default_start, min_value=overall_start, max_value=default_end)
    end_date = st.date_input("Select end Date", value=default_end, min_value=default_start, max_value=default_end)

    start_date = pd.to_datetime(start_date)
    end_date = pd.to_datetime(end_date)

    filtered_data = monthly_counts[(monthly_counts['year_month'] >= start_date) & 
        (monthly_counts['year_month'] <= end_date)]

    fig, ax = plt.subplots()
    ax.plot(filtered_data['year_month'], filtered_data['num_diagnoses'], marker='o', linestyle='-', color=colours[0])

    ax.set_xlabel("Month")
    ax.set_ylabel("New Diagnoses")
    ax.grid(True)

    st.pyplot(fig)