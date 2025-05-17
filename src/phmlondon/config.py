import os
from dotenv import load_dotenv
load_dotenv()

SNOWFLAKE_SERVER = os.getenv("SNOWFLAKE_SERVER")
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_USERGROUP = os.getenv("SNOWFLAKE_USERGROUP")

SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE", "INTELLIGENCE_DEV")
DEFINITION_LIBRARY = os.getenv("DEFINITION_LIBRARY", "AI_CENTRE_DEFINITION_LIBRARY")
EXTERNAL = os.getenv("EXTERNAL", "AI_CENTRE_EXTERNAL")
FEATURE_STORE = os.getenv("FEATURE_STORE", "AI_CENTRE_FEATURE_STORE")
FEATURE_METADATA = os.getenv("FEATURE_METADATA", "AI_CENTRE_FEATURE_STORE_METADATA")

# validate
def validate_config():
    required_vars = ["SNOWFLAKE_SERVER",
                     "SNOWFLAKE_USER",
                     "SNOWFLAKE_USERGROUP"]
    missing = [var for var in required_vars if not globals()[var]]

    if missing:
        raise EnvironmentError(f"Missing required env vars: {', '.join(missing)}")

    return True