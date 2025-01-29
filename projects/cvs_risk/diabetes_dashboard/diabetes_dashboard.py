"""
Streamlit app for diabetes dashboard
"""
from phmlondon.snow_utils import SnowflakeConnection
from dotenv import load_dotenv
import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt

@st.cache_resource
def connect() -> SnowflakeConnection:
    load_dotenv()
    conn = SnowflakeConnection()
    conn.use_database("INTELLIGENCE_DEV")
    conn.use_schema("AI_CENTRE_DEV")
    return conn

@st.cache_data
def load_data(_conn, table_name: str) -> pd.DataFrame:
    df = conn.session.table(table_name)
    df = df.to_pandas()
    df.columns = df.columns.str.lower()
    return df

st.title("NEL Diabetes Dashboard")

conn = connect()
df1 = load_data(conn, "PATIENTS_WITH_T2DM")
df2 = load_data(conn, "HBA1C_LEVELS")

box1, box2 = st.columns(2, border=True, vertical_alignment="bottom")
box3, box4 = st.columns(2, border=True, vertical_alignment="bottom")

colours = ["#003f5c", "#444e86", "#955196", "#dd5182", "#ff6e54", "#ffa600"]

with box1:
    st.subheader('Age')

    data_column = 'age_if_patient_living'
    # data_column = st.selectbox("Select a column to plot:", df1.select_dtypes(include='number').columns, index=5)
    bins = st.slider("Number of bins:", min_value=5, max_value=100, value=10)

    fig, ax = plt.subplots()
    ax.hist(df1[data_column], bins=bins, edgecolor='white', color=colours[0])
    ax.set_xlabel(data_column)
    ax.set_ylabel("Frequency")

    st.pyplot(fig)

# st.write(df1)

with box2:
    st.subheader('Gender and ethnicity')

    plot_choice = st.segmented_control("Select which to plot:", ["Gender", "Ethnicity"], default='Ethnicity')

    col_name = {'Gender': 'gender_as_text', 'Ethnicity': 'ethnicity_as_text_code'}

    counts = df1[col_name[plot_choice]].value_counts()

    fig2, ax2 = plt.subplots()
    ax2.bar(counts.index, counts.values, edgecolor=None, color=colours)
    ax2.set_xlabel(plot_choice)
    ax2.set_ylabel("Count")
    # ax2.set_title("Male vs Female Distribution")

    st.pyplot(fig2)

with box3:
    st.subheader('HbA1c levels')

    fig, ax = plt.subplots()
    ax.hist(df2['result_value'], bins=50, edgecolor='white', color=colours[2], range=(4,16))
    ax.set_xlabel('HbA1c level (%)')
    ax.set_ylabel("Frequency")

    st.pyplot(fig)

