import os

from dotenv import load_dotenv
import yaml
import glob
import pandas as pd
import streamlit as st

phenolab_config_mapping = {"SE56186": "nel_icb"}

def load_config() -> dict:
    load_dotenv(override=True)

    # Find out which snowflake account we're on
    account_name = st.session_state.session.sql("SELECT CURRENT_ACCOUNT();").collect()[0]["CURRENT_ACCOUNT()"]
    if account_name in phenolab_config_mapping:
        phenolab_config = phenolab_config_mapping[account_name]
    else:
        raise EnvironmentError("No matching configuration found for the current Snowflake account.")
    
    with open(f"configs/{phenolab_config}.yml", "r") as fid:
        config = yaml.safe_load(fid)

    if "local_development" not in config: # allow manual setting of local_development for debugging purposes, but in
        # production is should be set automatically by the below
        if os.getenv("GSETTINGS_SCHEMA_DIR") is not None:  
        # This is a hideous hack, but this env variable exists on streamlit in snowflake
            config["local_development"] = False
        else:
            config["local_development"] = True

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