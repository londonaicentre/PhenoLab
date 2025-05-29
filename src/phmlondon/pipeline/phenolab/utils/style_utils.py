import streamlit as st

def set_font_lato():
    st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Lato:wght@300&display=swap');

    * {
        font-family: 'Lato', sans-serif !important;
        font-weight: 300 !important;
    }
    html, body, h1, h2, h3, div, p, [class*="css"] {
        font-family: 'Lato', sans-serif !important;
        font-weight: 300 !important;
    }
    </style>
    """, unsafe_allow_html=True)