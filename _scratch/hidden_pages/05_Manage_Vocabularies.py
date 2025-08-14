import datetime
import os

import pandas as pd
import streamlit as st

from utils.database_utils import get_snowflake_session
from utils.style_utils import set_font_lato
from utils.config_utils import load_config

# # 01_Manage_Vocabularies.py

## THIS IS CURRENTLY DEPRECATED

# This page allows users to load an existing vocabulary or generate a new one from /
# Snowflake data sources. It extracts clinical codes (SNOMED, ICD10, OPCS4) with /
# usage statistics from clinical tables. /

# Codes that are in use + statistics, are joined onto a comprehensive codelist.
# - For SNOMED, the full codelist is taken from the DDS CONCEPT table.
# - For ICD10/OPCS4, this is taken from the canonical Athena (OMOP) release.

# Vocabularies are saved as parquet files for efficient storage and quick loading. /
# The same base vocabulary can be used between different locations.


# SNOMED stats from OBSERVATION
OBSERVATION_SNOMED_SQL = """
SELECT
    c.NAME AS CODE_DESCRIPTION,
    c.CODE AS CODE,
    COUNT(o.ID) AS CODE_COUNT,
    'REFERENCE_DDS_SNOMED' AS CODE_TYPE,
    'SNOMED' AS VOCABULARY,
    -- Quartile values
    APPROX_PERCENTILE(TRY_CAST(o.RESULT_VALUE AS FLOAT), 0.25) AS LQ_VALUE,
    APPROX_PERCENTILE(TRY_CAST(o.RESULT_VALUE AS FLOAT), 0.5) AS MEDIAN_VALUE,
    APPROX_PERCENTILE(TRY_CAST(o.RESULT_VALUE AS FLOAT), 0.75) AS UQ_VALUE,
    -- Percentage of non-null result values
    (COUNT(o.RESULT_VALUE) * 100.0 / NULLIF(COUNT(o.ID), 0)) AS PERCENT_HAS_RESULT_VALUE,
    -- Age percentiles
    APPROX_PERCENTILE(o.AGE_AT_EVENT, 0.25) AS LQ_AGE,
    APPROX_PERCENTILE(o.AGE_AT_EVENT, 0.5) AS MEDIAN_AGE,
    APPROX_PERCENTILE(o.AGE_AT_EVENT, 0.75) AS UQ_AGE,
    -- Year distribution
    COUNT_IF(EXTRACT(YEAR FROM o.CLINICAL_EFFECTIVE_DATE) IN (2015, 2016)) AS COUNT_2015_2016,
    COUNT_IF(EXTRACT(YEAR FROM o.CLINICAL_EFFECTIVE_DATE) IN (2017, 2018)) AS COUNT_2017_2018,
    COUNT_IF(EXTRACT(YEAR FROM o.CLINICAL_EFFECTIVE_DATE) IN (2019, 2020)) AS COUNT_2019_2020,
    COUNT_IF(EXTRACT(YEAR FROM o.CLINICAL_EFFECTIVE_DATE) IN (2021, 2022)) AS COUNT_2021_2022,
    COUNT_IF(EXTRACT(YEAR FROM o.CLINICAL_EFFECTIVE_DATE) IN (2023, 2024)) AS COUNT_2023_2024
FROM PROD_DWH.ANALYST_PRIMARY_CARE.CONCEPT c
LEFT JOIN PROD_DWH.ANALYST_PRIMARY_CARE.OBSERVATION o
    ON o.CORE_CONCEPT_ID = c.DBID
WHERE c.SCHEME = 71
GROUP BY c.NAME, c.CODE, c.SCHEME_NAME
ORDER BY CODE_COUNT DESC
"""

# ICD10/OPCS4 stats from BASE_APC_CONCEPTS
HOSPITAL_CODES_SQL = """
SELECT
    CONCEPT_CODE_STD AS CODE,
    COUNT(*) AS CODE_COUNT,
    'REFERENCE_ICD10_OPCS4' AS CODE_TYPE,
    VOCABULARY,
    -- No value quartiles for hospital codes
    NULL AS LQ_VALUE,
    NULL AS MEDIAN_VALUE,
    NULL AS UQ_VALUE,
    NULL AS PERCENT_HAS_RESULT_VALUE,
    -- Age percentiles
    APPROX_PERCENTILE(PATIENT_AGE, 0.25) AS LQ_AGE,
    APPROX_PERCENTILE(PATIENT_AGE, 0.5) AS MEDIAN_AGE,
    APPROX_PERCENTILE(PATIENT_AGE, 0.75) AS UQ_AGE,
    -- Year distribution
    COUNT_IF(EXTRACT(YEAR FROM ACTIVITY_DATE) IN (2015, 2016)) AS COUNT_2015_2016,
    COUNT_IF(EXTRACT(YEAR FROM ACTIVITY_DATE) IN (2017, 2018)) AS COUNT_2017_2018,
    COUNT_IF(EXTRACT(YEAR FROM ACTIVITY_DATE) IN (2019, 2020)) AS COUNT_2019_2020,
    COUNT_IF(EXTRACT(YEAR FROM ACTIVITY_DATE) IN (2021, 2022)) AS COUNT_2021_2022,
    COUNT_IF(EXTRACT(YEAR FROM ACTIVITY_DATE) IN (2023, 2024)) AS COUNT_2023_2024
FROM {database}.{feature_store}.BASE_APC_CONCEPTS
WHERE CONCEPT_CODE_STD IS NOT NULL
GROUP BY CONCEPT_CODE_STD, VOCABULARY
ORDER BY CODE_COUNT DESC
"""

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
    - Primary care observations (SNOMED)
    - Athena reference concepts joined to hospital usage statistics (IC10/OPCS4)
    """
    try:
        # updating status
        status_placeholder = st.empty()

        concept_dfs = []

        # 1. Primary care observations (SNOMED)
        status_placeholder.info("Extracting primary care observation SNOMED codes...")

        # session.use_database("PROD_DWH")
        # session.use_schema("ANALYST_PRIMARY_CARE")
        observation_df = st.session_state.session.sql(OBSERVATION_SNOMED_SQL).to_pandas()
        # Use context manager for PROD_DWH database
        # with conn.use_context(database="PROD_DWH", schema="ANALYST_PRIMARY_CARE"):
        #     observation_df = conn.execute_query_to_df(OBSERVATION_SNOMED_SQL)
        concept_dfs.append(observation_df)
        status_placeholder.success(f"Extracted {len(observation_df)} primary care observation SNOMED codes")

        # 2. Load Athena reference ICD10/OPCS4 codes (as the base)
        status_placeholder.info("Loading Athena reference ICD10/OPCS4 codes...")
        athena_codes_path = "data/athena/2025_ICD10_OPCS4.parquet"

        try:
            athena_df = pd.read_parquet(athena_codes_path)
            athena_df = athena_df[athena_df["valid_end_date"] == 20991231]

            reference_df = pd.DataFrame({
                "CODE": athena_df["concept_code"],
                "CODE_DESCRIPTION": athena_df["concept_name"],
                "VOCABULARY": athena_df["vocabulary_id"],
                "CODE_TYPE": "REFERENCE_ICD10_OPCS4"
            })
            status_placeholder.info(f"Loaded {len(reference_df)} Athena reference codes")
            print(reference_df)
            # 3. Extract hospital usage statistics for these codes
            status_placeholder.info("Extracting hospital usage statistics...")

            # Use context manager for feature store database
            # with conn.use_context(database=st.session_state.config[feature_store]["database"], schema=st.session_state.config[feature_store]["schema"]):
            #     hospital_query = HOSPITAL_CODES_SQL.format(database=st.session_state.config[feature_store]["database"], feature_store=st.session_state.config[feature_store]["schema"])
            #     hospital_df = conn.execute_query_to_df(hospital_query)
            hospital_query = HOSPITAL_CODES_SQL.format(
                database=st.session_state.config["schema"]["database"], 
                feature_store=st.session_state.config["schema"]["schema"])
            hospital_df = st.session_state.session.sql(hospital_query).to_pandas()

            print(hospital_df)

            if not hospital_df.empty:
                status_placeholder.success(f"Found usage statistics for {len(hospital_df)} hospital codes")

                # 4. Left join Athena reference with hospital statistics
                stat_columns = [
                    "CODE", "VOCABULARY", "CODE_COUNT",
                    "LQ_VALUE", "MEDIAN_VALUE", "UQ_VALUE", "PERCENT_HAS_RESULT_VALUE",
                    "LQ_AGE", "MEDIAN_AGE", "UQ_AGE",
                    "COUNT_2015_2016", "COUNT_2017_2018", "COUNT_2019_2020",
                    "COUNT_2021_2022", "COUNT_2023_2024"
                ]
                hospital_stats_df = hospital_df[stat_columns].copy()
                print(hospital_stats_df)

                reference_df = reference_df.merge(
                    hospital_stats_df,
                    how='left',
                    left_on=['CODE', 'VOCABULARY'],
                    right_on=['CODE', 'VOCABULARY']
                )

                # Fill NaN values with appropriate defaults
                reference_df["CODE_COUNT"] = reference_df["CODE_COUNT"].fillna(0)
                for count_col in [
                    "COUNT_2015_2016", "COUNT_2017_2018", "COUNT_2019_2020",
                    "COUNT_2021_2022", "COUNT_2023_2024"
                ]:
                    reference_df[count_col] = reference_df[count_col].fillna(0)

                status_placeholder.success("Successfully joined Athena codes with hospital statistics")
            else:
                status_placeholder.warning("No hospital code usage statistics found")
                raise

            # now add to concepts_df
            concept_dfs.append(reference_df)

        except Exception as e:
            status_placeholder.error(f"Error processing Athena reference codes: {e}")
            raise e

        # 5. Union all vocabulary sources
        status_placeholder.info("Combining to Master Vocabulary...")
        combined_df = pd.concat(concept_dfs, ignore_index=True)

        # 6. Save
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
    st.set_page_config(page_title="Manage Vocabularies", layout="wide")

    set_font_lato()
    if "session" not in st.session_state:
        st.session_state.session = get_snowflake_session()
    if "config" not in st.session_state:
        st.session_state.config = load_config()

    st.title("Manage Vocabularies")

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
                    st.success(f"Generated new vocabulary and saved to {os.path.basename(file_path)}")
                except Exception as e:
                    st.error(f"Failed to generate vocabulary: {e}")

    # display the vocabulary
    if st.session_state.codes is not None:
        st.subheader("Currently Loaded Vocabulary")

        df_display = st.session_state.codes.head(100)
        st.dataframe(df_display, use_container_width=True)

        total_codes = len(st.session_state.codes)
        st.info(f"Showing top 100 of {total_codes:,} total codes")
    else:
        st.info("Please load an existing vocabulary or generate a new one.")


if __name__ == "__main__":
    main()
