import datetime
import os

import pandas as pd
import streamlit as st
from dotenv import load_dotenv

from phmlondon.snow_utils import SnowflakeConnection


def load_concept_list(file_path):
    """
    Load a concept list from a parquet file
    """
    try:
        df = pd.read_parquet(file_path)
        return df
    except Exception as e:
        st.error(f"Unable to load concept list: {e}")
        raise e

def generate_concept_list():
    """
    Generate a new concept list from multiple data sources:
    - Primary care observations
    - Secondary care diagnoses (ICD10)
    """
    try:
        # updating status (previously boxes were stacking)
        status_placeholder = st.empty()

        snowsesh = SnowflakeConnection()
        snowsesh.use_database("PROD_DWH")

        concept_dfs = []

        # 1. primary care observations
        status_placeholder.info("Extracting primary care observation concepts...")
        snowsesh.use_schema("ANALYST_PRIMARY_CARE")

        with open("sql/observation_concepts.sql", "r") as file:
            observation_query = file.read()

        observation_df = snowsesh.execute_query_to_df(observation_query)
        concept_dfs.append(observation_df)
        status_placeholder.success(f"Extracted {len(observation_df)} primary care observation concepts")

        # 2. ICD10 from SUS
        status_placeholder.info("Extracting secondary care ICD10 diagnosis concepts...")
        snowsesh.use_schema("ANALYST_FACTS_UNIFIED_SUS")

        with open("sql/icd10_concepts.sql", "r") as file:
            icd10_query = file.read()

        icd10_df = snowsesh.execute_query_to_df(icd10_query)
        concept_dfs.append(icd10_df)
        status_placeholder.success(f"Extracted {len(icd10_df)} ICD10 diagnosis concepts")

        # 3. OPCS-4 procedures from SUS
        status_placeholder.info("Extracting secondary care OPCS-4 procedure concepts...")
        with open("sql/opcs4_concepts.sql", "r") as file:
            opcs4_query = file.read()

        opcs4_df = snowsesh.execute_query_to_df(opcs4_query)
        concept_dfs.append(opcs4_df)
        status_placeholder.success(f"Extracted {len(opcs4_df)} OPCS-4 procedure concepts")

        # 4. SNOMED from ECDS
        status_placeholder.info("Extracting emergency care SNOMED concepts...")
        with open("sql/ecds_snomed_concepts.sql", "r") as file:
            ecds_query = file.read()

        ecds_df = snowsesh.execute_query_to_df(ecds_query)
        concept_dfs.append(ecds_df)
        status_placeholder.success(f"Extracted {len(ecds_df)} SNOMED emergency care concepts")

        # 5. Union
        status_placeholder.info("Combining concepts from all sources...")
        combined_df = pd.concat(concept_dfs, ignore_index=True)

        # 6. Validate
        required_columns = ['CONCEPT_CODE',
                            'CONCEPT_NAME',
                            'CONCEPT_COUNT',
                            'VOCABULARY',
                            'CONCEPT_TYPE',
                            'LQ_VALUE',
                            'MEDIAN_VALUE',
                            'UQ_VALUE',
                            'PERCENT_HAS_RESULT_VALUE',
                            'RESULT_UNITS_ARRAY',
                            'LQ_AGE',
                            'MEDIAN_AGE',
                            'UQ_AGE',
                            'PCT_2015_2016',
                            'PCT_2017_2018',
                            'PCT_2019_2020',
                            'PCT_2021_2022',
                            'PCT_2023_2024']
        for col in required_columns:
            if col not in combined_df.columns:
                status_placeholder.error(f"Missing required column: {col}")
                raise ValueError(f"Missing required column: {col}")

        # 5. save
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        file_name = f"concepts_{timestamp}.parquet"
        file_path = os.path.join("data/concepts", file_name)

        os.makedirs("data/concepts", exist_ok=True)
        combined_df.to_parquet(file_path)

        status_placeholder.success(f"Generated combined concept list with {len(combined_df)} total concepts")
        return combined_df, file_path

    except Exception as e:
        st.error(f"Unable to generate concept list: {e}")
        raise e

def main():
    st.set_page_config(page_title="Concept List Creator", layout="wide")

    st.title("Concept List Creator")

    load_dotenv()

    # session state variables
    if "concepts" not in st.session_state:
        st.session_state.concepts = None

    # display layout
    col1, col2 = st.columns([1, 1])

    with col1:
        # list for dropdown component
        concept_files = []
        try:
            if os.path.exists("data/concepts"):
                concept_files = [f for f in os.listdir("data/concepts") if f.endswith(".parquet")]
        except Exception as e:
            st.error(f"Can't list concept files: {e}")

        # component: concept list dropdown
        selected_file = st.selectbox(
            "Concept list", options=concept_files, index=0 if concept_files else None
        )

        # component: load concept file
        if selected_file and st.button("Load concept list"):
            with st.spinner("Loading concept list..."):
                file_path = os.path.join("data/concepts", selected_file)
                st.session_state.concepts = load_concept_list(file_path)
                st.success(f"Loaded concept list: {selected_file}")

    with col2:
        # component: create new concept list
        if st.button("Regenerate concept list"):
            with st.spinner("Generating new concept list from Snowflake..."):
                try:
                    df, file_path = generate_concept_list()
                    st.session_state.concepts = df
                    st.success(
                        f"Generated new concept list and saved to {os.path.basename(file_path)}"
                    )
                except Exception as e:
                    st.error(f"Failed to generate concept list: {e}")

    # display the concept list
    if st.session_state.concepts is not None:
        st.subheader("Concept List")
        df_display = st.session_state.concepts.head(100)
        st.dataframe(df_display, use_container_width=True)

        total_concepts = len(st.session_state.concepts)
        st.info(f"Showing top 100 of {total_concepts} total concepts")
    else:
        st.info("Please load an existing concept list or generate a new one.")


if __name__ == "__main__":
    main()
