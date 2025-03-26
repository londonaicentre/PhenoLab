import datetime
import os

import pandas as pd
import streamlit as st
from dotenv import load_dotenv

from phmlondon.snow_utils import SnowflakeConnection


@st.cache_data(ttl=3600)  # cache for 1hr
def load_vocab_list(file_path):
    """
    Load a vocabulary from a parquet file
    """
    try:
        df = pd.read_parquet(file_path)
        return df
    except Exception as e:
        st.error(f"Unable to load vocabulary: {e}")
        raise e

def generate_vocab_list():
    """
    Generate a new vocabulary from multiple data sources:
    - Primary care observations
    - Secondary care diagnoses (ICD10)
    """
    try:
        # updating status (previously boxes were stacking)
        status_placeholder = st.empty()

        snowsesh = SnowflakeConnection()
        snowsesh.use_database("PROD_DWH")

        concept_dfs = []

        # 1. Primary care observations
        status_placeholder.info("Extracting primary care observation SNOMED codes...")
        snowsesh.use_schema("ANALYST_PRIMARY_CARE")

        with open("sql/observation_snomed.sql", "r") as file:
            observation_query = file.read()

        observation_df = snowsesh.execute_query_to_df(observation_query)
        concept_dfs.append(observation_df)
        status_placeholder.success(f"Extracted {len(observation_df)} primary care observation SNOMED codes")

        ## FOR NOW - EXCLUDING CODES/COUNTS FROM SUS
        # 2. ICD10 from SUS
        # status_placeholder.info("Extracting secondary care ICD10 diagnosis codes...")
        # snowsesh.use_schema("ANALYST_FACTS_UNIFIED_SUS")

        # with open("sql/sus_icd10.sql", "r") as file:
        #     icd10_query = file.read()

        # icd10_df = snowsesh.execute_query_to_df(icd10_query)
        # concept_dfs.append(icd10_df)
        # status_placeholder.success(f"Extracted {len(icd10_df)} ICD10 diagnosis codes")

        # 3. OPCS-4 procedures from SUS
        # status_placeholder.info("Extracting secondary care OPCS-4 procedure codes...")
        # with open("sql/sus_opcs4.sql", "r") as file:
        #     opcs4_query = file.read()

        # opcs4_df = snowsesh.execute_query_to_df(opcs4_query)
        # concept_dfs.append(opcs4_df)
        # status_placeholder.success(f"Extracted {len(opcs4_df)} OPCS-4 procedure codes")

        # 4. SNOMED from ECDS
        # status_placeholder.info("Extracting emergency care SNOMED codes...")
        # with open("sql/ecds_snomed.sql", "r") as file:
        #     ecds_query = file.read()

        # ecds_df = snowsesh.execute_query_to_df(ecds_query)
        # concept_dfs.append(ecds_df)
        # status_placeholder.success(f"Extracted {len(ecds_df)} SNOMED emergency care codes")

        # 5. Add Athena reference ICD10/OPCS4 codes
        status_placeholder.info("Loading reference ICD10/OPCS4 codes...")
        try:
            athena_codes_path = "data/athena/2025_ICD10_OPCS4.parquet"
            athena_df = pd.read_parquet(athena_codes_path)
            athena_df = athena_df[athena_df['valid_end_date'] == 20991231]

            reference_df = pd.DataFrame({
                'CODE': athena_df['concept_code'],
                'CODE_DESCRIPTION': athena_df['concept_name'],
                'CODE_COUNT': 1,  # reference codes
                'VOCABULARY': athena_df['vocabulary_id'],
                'CODE_TYPE': 'REFERENCE_ICD10_OPCS4',
                # reference codes
                'LQ_VALUE': None,
                'MEDIAN_VALUE': None,
                'UQ_VALUE': None,
                'PERCENT_HAS_RESULT_VALUE': None,
                'RESULT_UNITS_ARRAY': None,
                'LQ_AGE': None,
                'MEDIAN_AGE': None,
                'UQ_AGE': None,
                'PCT_2015_2016': None,
                'PCT_2017_2018': None,
                'PCT_2019_2020': None,
                'PCT_2021_2022': None,
                'PCT_2023_2024': None
            })
            concept_dfs.append(reference_df)
            status_placeholder.success(f"Loaded {len(reference_df)} reference ICD10/OPCS4 codes")
        except Exception as e:
            status_placeholder.warning(f"Could not load reference ICD10/OPCS4 codes: {e}")

        # 6. Union
        status_placeholder.info("Combining to Master Vocabulary...")
        combined_df = pd.concat(concept_dfs, ignore_index=True)

        # 7. Validate
        required_columns = ['CODE',
                            'CODE_DESCRIPTION',
                            'CODE_COUNT',
                            'VOCABULARY',
                            'CODE_TYPE',
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

        # 5. Save
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        file_name = f"vocab_{timestamp}.parquet"
        file_path = os.path.join("data/vocab", file_name)

        os.makedirs("data/vocab", exist_ok=True)
        combined_df.to_parquet(file_path)

        status_placeholder.success(f"Generated combined vocabulary with {len(combined_df)} total codes")
        return combined_df, file_path

    except Exception as e:
        st.error(f"Unable to generate vocabulary: {e}")
        raise e

def main():
    st.set_page_config(page_title="Load or Generate Vocabulary", layout="wide")

    st.title("Load or Generate Vocabulary")

    load_dotenv()

    # session state variables
    if "codes" not in st.session_state:
        st.session_state.codes = None

    # display layout
    col1, col2 = st.columns([1, 1])

    with col1:
        # list for dropdown component
        concept_files = []
        try:
            if os.path.exists("data/vocab"):
                concept_files = [f for f in os.listdir("data/vocab") if f.endswith(".parquet")]
        except Exception as e:
            st.error(f"Can't list concept files: {e}")

        # component: vocabulary dropdown
        selected_file = st.selectbox(
            "Concept list", options=concept_files, label_visibility="collapsed", index=0 if concept_files else None
        )

    with col2:
        # component: load concept file
        if selected_file and st.button("Load vocabulary"):
            with st.spinner("Loading vocab..."):
                file_path = os.path.join("data/vocab", selected_file)
                st.session_state.codes = load_vocab_list(file_path)
                with col1:
                    st.success(f"Loaded vocab: {selected_file}")

    with col1:
        # component: create new vocabulary
        if st.button("Regenerate vocabulary"):
            with st.spinner("Generating new vocabulary from Snowflake..."):
                try:
                    df, file_path = generate_vocab_list()
                    st.session_state.codes = df
                    st.success(
                        f"Generated new vocabulary and saved to {os.path.basename(file_path)}"
                    )
                except Exception as e:
                    st.error(f"Failed to generate vocabulary: {e}")

    # display the vocabulary
    if st.session_state.codes is not None:
        st.subheader("Vocabulary")
        df_display = st.session_state.codes.head(100)
        st.dataframe(df_display, use_container_width=True)

        total_codes = len(st.session_state.codes)
        st.info(f"Showing top 100 of {total_codes} total codes")
    else:
        st.info("Please load an existing vocabulary or generate a new one.")


if __name__ == "__main__":
    main()
