import os

import yaml
from dotenv import load_dotenv
import glob
import pandas as pd
import streamlit as st
from snowflake.snowpark import Session

def load_phenolab_config_mapping():
    """
    Load mappings configured in yml file
    """
    with open("configs/account_mapping.yml", "r") as fid:
        mapping_config = yaml.safe_load(fid)
    return mapping_config["account_mappings"]

def load_config(session: Session = None, deploy_env: str = None) -> dict:
    """
    Load the configuration file based on the current Snowflake account and environment.

    Args:
        session (Session): Optional Snowflake session to determine the account name. If not provided, it will use the
        session from Streamlit's state. If calling from within Streamlit, leave as None to use the session state.

        deploy_env (str): Optional environment to use (e.g., 'dev', 'prod'). If not provided, the function will look
        for it in the environment variables. If still not found, it defaults to 'prod' for local development and 'dev
        for remote
    """

    load_dotenv(override=True)
    if os.environ['HOME'] == "/home/udf":
        # This is a hideous hack, but this env variable exists on streamlit in snowflake
        local_development = False
    else:
        local_development = True

    deploy_env = deploy_env or os.getenv("DEPLOY_ENV")
    if deploy_env is None:
        deploy_env = "dev"  # always use dev as default

    print(f"Running in environment: {deploy_env}")

    # Find out which snowflake account we're on
    session = session or st.session_state.session
    account_name = session.sql("SELECT CURRENT_ACCOUNT();").collect()[0]["CURRENT_ACCOUNT()"]
    phenolab_config_mapping = load_phenolab_config_mapping()
    if account_name in phenolab_config_mapping:
        phenolab_config = phenolab_config_mapping[account_name] + "_" + deploy_env
    else:
        raise EnvironmentError("No matching configuration found for the current Snowflake account.")

    with open(f"configs/{phenolab_config}.yml", "r") as fid:
        config = yaml.safe_load(fid)

    config["deploy_env"] = deploy_env
    config["icb_name"] = phenolab_config_mapping[account_name]
    config["local_development"] = local_development

    return config

def preload_vocabulary():
    """
    Load vocabulary from Snowflake table.
    """
    if "session" not in st.session_state or "config" not in st.session_state:
        return False, "Session or config not initialized"

    session = st.session_state.session
    config = st.session_state.config

    if "vocabulary_table" not in config:
        return False, "Vocabulary table not configured"

    query = f"""
    SELECT
        CONCEPT_NAME as CODE_DESCRIPTION,
        CONCEPT_CODE as CODE,
        CONCEPT_VOCABULARY as VOCABULARY,
        CONCEPT_CODE_COUNT as CODE_COUNT,
        UNIQUE_PATIENT_COUNT,
        LQ_VALUE,
        MEDIAN_VALUE,
        UQ_VALUE,
        PERCENT_HAS_RESULT_VALUE
    FROM {config['vocabulary_table']}
    WHERE CONCEPT_CODE IS NOT NULL
    """

    try:
        vocab_df = session.sql(query).to_pandas()
        st.session_state.codes = vocab_df
        return True, f"Vocabulary loaded ({len(vocab_df):,} codes)"
    except Exception as e:
        return False, f"Failed to load vocabulary: {e}"

if __name__ == "__main__":
    # Example usage/for debugging
    config = load_config(session=Session.builder.config("connection_name", "nel_icb").create())
    print(config)