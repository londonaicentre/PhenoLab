# Standalone test script to see if I can connect to snowflake

import logging

from dotenv import load_dotenv

from phmlondon.snow_utils import SnowflakeConnection

logging.basicConfig(level=logging.DEBUG)

load_dotenv()
snowsesh = SnowflakeConnection()
snowsesh.use_database("INTELLIGENCE_DEV")
snowsesh.use_schema("AI_CENTRE_DEV")

snowsesh.list_tables()
