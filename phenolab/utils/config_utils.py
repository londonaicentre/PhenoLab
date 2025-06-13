import os

from dotenv import load_dotenv
import yaml
from snowflake.snowpark import Session

phenolab_config_mapping = {"SE56186": "nel_icb"}

def load_config(session: Session) -> dict:
    load_dotenv(override=True)

    # Find out which snowflake account we're on
    account_name = session.sql("SELECT CURRENT_ACCOUNT();").collect()[0]["CURRENT_ACCOUNT()"]
    if account_name in phenolab_config_mapping:
        phenolab_config = phenolab_config_mapping[account_name]
    else:
        raise EnvironmentError("No matching configuration found for the current Snowflake account.")
    
    with open(f"configs/{phenolab_config}.yml", "r") as fid:
        config = yaml.safe_load(fid)

        # Set whether we are developing locally or for the AI centre
    # if os.getenv("LOCAL_DEVELOPMENT", "FALSE").upper() == "TRUE":
    #     config["local_development"] = True
    # else:
    #     config["local_development"] = False

    if os.getenv("GSETTINGS_SCHEMA_DIR") is not None:  
    # This is a hideous hack, but this env variable exists on streamlit in snowflake
        config["local_development"] = False
    else:
        config["local_development"] = True

    return config

if __name__ == "__main__":
    # Example usage/for debugging
    config = load_config(session=Session.builder.config("connection_name", "nel_icb").create())
    print(config)