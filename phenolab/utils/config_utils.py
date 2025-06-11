import os

from dotenv import load_dotenv
import yaml

def load_config() -> dict:
    load_dotenv(override=True)

    phenolab_config = os.getenv("PHENOLAB_CONFIG")
    if phenolab_config is None:
        raise EnvironmentError("PHENOLAB_CONFIG environment variable is not set.")
    with open(f"configs/{phenolab_config}.yml", "r") as fid:
        return yaml.safe_load(fid)

if __name__ == "__main__":
    # Example usage/for debugging
    config = load_config()
    print(config)