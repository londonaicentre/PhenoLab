from phmlondon.snow_utils import SnowflakeConnection

snowsesh = SnowflakeConnection()
snowsesh.use_database("INTELLIGENCE_DEV")

snowsesh.execute_query("CREATE SCHEMA IF NOT EXISTS AI_CENTRE_PHENOTYPE_LIBRARY")
snowsesh.use_schema("AI_CENTRE_PHENOTYPE_LIBRARY")

