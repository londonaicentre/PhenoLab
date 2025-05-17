import os
from dotenv import load_dotenv
load_dotenv()

SNOWFLAKE_SERVER = os.getenv("SNOWFLAKE_SERVER")
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_USERGROUP = os.getenv("SNOWFLAKE_USERGROUP")

# AI Centre targets
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE", "INTELLIGENCE_DEV")
DEFINITION_LIBRARY = os.getenv("DEFINITION_LIBRARY", "AI_CENTRE_DEFINITION_LIBRARY")
EXTERNAL = os.getenv("EXTERNAL", "AI_CENTRE_EXTERNAL")
FEATURE_STORE = os.getenv("FEATURE_STORE", "AI_CENTRE_FEATURE_STORE")
FEATURE_METADATA = os.getenv("FEATURE_METADATA", "AI_CENTRE_FEATURE_STORE_METADATA")

# GP base observation table
DDS_OBSERVATION = os.getenv("DDS_OBSERVATION", "PROD_DWH.ANALYST_PRIMARY_CARE.OBSERVATION")
OB_COREID = os.getenv("OB_COREID", "CORE_CONCEPT_ID")
OB_DATE = os.getenv("OB_DATE", "CLINICAL_EFFECTIVE_DATE")
OB_PERSON = os.getenv("OB_PERSON", "PERSON_ID")
OB_RESULT = os.getenv("OB_RESULT", "RESULT_VALUE")
OB_RESULT_UNIT = os.getenv("OB_RESULT_UNIT", "RESULT_VALUE_UNITS")
OB_ORGANISATION = os.getenv("OB_ORGANISATION", "ORGANIZATION_ID")

# GP concept lookup
DDS_CONCEPT = os.getenv("DDS_CONCEPT", "PROD_DWH.ANALYST_PRIMARY_CARE.CONCEPT")
DDS_CODE = os.getenv("DDS_CODE", "CODE")
DDS_NAME = os.getenv("DDS_NAME", "DESCRIPTION")
DDS_VOCAB = os.getenv("DDS_VOCAB", "SCHEME")
DDS_SNOMED = os.getenv("DDS_SNOMED", "71")
DDS_COREID = os.getenv("DDS_COREID", "DBID")

# validate
def validate_config():
    required_vars = ["SNOWFLAKE_SERVER",
                     "SNOWFLAKE_USER",
                     "SNOWFLAKE_USERGROUP"]
    missing = [var for var in required_vars if not globals()[var]]

    if missing:
        raise EnvironmentError(f"Missing required env vars: {', '.join(missing)}")

    return True