import streamlit as st
import requests
from dotenv import load_dotenv
import os
from phenolab.utils.definition import Definition

if st.button("Submit"):

    load_dotenv()

    headers = {"x-api-key": os.getenv("INTERNAL_API_KEY")}

    diabetes = Definition.from_json('../data/definitions/diabetes_mellitus_not_type1_SNOMED_7b322f7f.json')
    arm_fracture = Definition.from_json('../data/definitions/arm_fracture_ICD10_9cb7ac20.json')

    response = requests.post("http://localhost:5000/save-definition", json=arm_fracture.to_dict(), headers=headers)
    if response.ok:
        st.success(f"{response.text} - {response.status_code}")
    else:
        st.error(f"{response.text}")
