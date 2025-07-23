import os

import yaml
from dotenv import load_dotenv
import glob
import pandas as pd
import streamlit as st
from snowflake.snowpark import Session

phenolab_config_mapping = {"SE56186": "nel_icb" }

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
        if local_development: # default is prod for local development and dev for remote
            deploy_env = "prod"
        else:
            deploy_env = "dev"

    print(f"Running in environment: {deploy_env}")

    # Find out which snowflake account we're on
    session = session or st.session_state.session
    account_name = session.sql("SELECT CURRENT_ACCOUNT();").collect()[0]["CURRENT_ACCOUNT()"]
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
    Preload the most recent vocabulary file if available.
    """
    vocab_dir = "data/vocab"
    if not os.path.exists(vocab_dir):
        return False, "Vocabulary directory not found"

    vocab_files = glob.glob(os.path.join(vocab_dir, "vocab_*.parquet"))

    if not vocab_files:
        return False, "No vocabulary files found. Please generate a new vocabulary."

    # sort by filename (yyyy-mm-dd)
    most_recent_file = sorted(vocab_files)[-1]

    vocab_df = pd.read_parquet(most_recent_file)

    st.session_state.codes = vocab_df

    return True, "Vocabulary loaded"

if __name__ == "__main__":
    # Example usage/for debugging
    config = load_config(session=Session.builder.config("connection_name", "nel_icb").create())
    print(config)