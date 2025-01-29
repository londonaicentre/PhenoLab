"""
Streamlit app for diabetes dashboard
"""
from phmlondon.snow_utils import SnowflakeConnection
from dotenv import load_dotenv
import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt

@st.cache_resource
def connect():
    load_dotenv()
    conn = SnowflakeConnection()
    conn.use_database("INTELLIGENCE_DEV")
    conn.use_schema("AI_CENTRE_DEV")
    return conn

@st.cache_data
def load_data(_conn) -> pd.DataFrame:
    df = conn.session.table("PATIENTS_WITH_T2DM")
    return df.to_pandas()

st.title("NEL Diabetes Dashboard")

conn = connect()
df = load_data(conn)
df.columns = df.columns.str.lower()

col1, col2 = st.columns(2)

with col1:
    st.subheader('Select data to plot as a histogram')

    data_column = st.selectbox("Select a column to plot:", df.select_dtypes(include='number').columns, index=5)
    bins = st.slider("Number of bins:", min_value=5, max_value=100, value=10)

    fig, ax = plt.subplots()
    ax.hist(df[data_column], bins=bins, edgecolor='black')
    ax.set_title(f"Histogram of {data_column}")
    ax.set_xlabel(data_column)
    ax.set_ylabel("Frequency")

    st.pyplot(fig)

# st.write(df)

with col2:
    st.subheader('Gender breakdown')

    gender_counts = df["gender_as_text"].value_counts()

    # Create bar chart
    fig2, ax2 = plt.subplots()
    ax2.bar(gender_counts.index, gender_counts.values, color=["blue", "pink"], edgecolor='black')
    ax2.set_xlabel("Gender")
    ax2.set_ylabel("Count")
    ax2.set_title("Male vs Female Distribution")

    # Display in Streamlit
    st.pyplot(fig2)


