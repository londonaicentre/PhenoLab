from dotenv import load_dotenv
from phmlondon.snow_utils import SnowflakeConnection
from datetime import datetime
import pandas as pd
import json

# Generates the following PERSON_PHENOTYPE tables
# INTELLIGENCE_DEV.AI_CENTRE_FEATURE_STORE.PERSON_PHENOTYPE_BY_YEAR
#   > For every PERSON ever registered in NEL, flags 1 if positive for a given phenotype in a given year
# INTELLIGENCE_DEV.AI_CENTRE_FEATURE_STORE.PERSON_5YEAR_PHENOTYPE
#   > For every active PERSON (alive + registered) in NEL, flags 1 for phenotype if present in past 5 years
# INTELLIGENCE_DEV.AI_CENTRE_FEATURE_STORE.PERSON_PHENOTYPE_AGE_OF_ONSET
#   > For every PERSON ever registered in NEL, calculages Age that phenotype was first recorded, for each phenotype

CREATE_YEARLY_TABLE_SQL = """
CREATE OR REPLACE TABLE INTELLIGENCE_DEV.AI_CENTRE_FEATURE_STORE.PERSON_PHENOTYPE_BY_YEAR AS
WITH phenotype_concepts AS (
    -- Get all relevant concept IDs for our phenotypes of interest
    SELECT DISTINCT
        PHENOTYPE_NAME,
        CORE_CONCEPT_ID
    FROM INTELLIGENCE_DEV.AI_CENTRE_PHENOTYPE_LIBRARY.PHENOSTORE
    WHERE (PHENOTYPE_SOURCE = 'LONDON' OR PHENOTYPE_SOURCE = 'ICB_NEL')
    AND PHENOTYPE_NAME IN ({phenotype_list})
    AND CORE_CONCEPT_ID IS NOT NULL
),
year_observations AS (
    -- Get all observations by year for these concepts
    SELECT DISTINCT
        o.PERSON_ID,
        YEAR(o.CLINICAL_EFFECTIVE_DATE) as OBSERVATION_YEAR,
        pc.PHENOTYPE_NAME
    FROM PROD_DWH.ANALYST_PRIMARY_CARE.OBSERVATION o
    INNER JOIN phenotype_concepts pc
        ON o.CORE_CONCEPT_ID = pc.CORE_CONCEPT_ID
    WHERE o.CLINICAL_EFFECTIVE_DATE IS NOT NULL
)
SELECT
    yo.PERSON_ID,
    yo.OBSERVATION_YEAR,
    pd.GENDER,
    pd.ETHNIC_AIC_CATEGORY,
    pd.DATE_OF_BIRTH,
    pd.LONDON_IMD_RANK,
    pd.LONDON_IMD_DECILE,
    pd.IMD_QUINTILE,
    pd.IDACI_LONDON_RANK,
    pd.IDACI_LONDON_DECILE,
    pd.IDAOP_LONDON_RANK,
    pd.IDAOP_LONDON_DECILE,
    pd.PATIENT_STATUS,
    pd.INCLUDE_IN_LIST_SIZE_FLAG,
    {phenotype_columns}
FROM year_observations yo
LEFT JOIN INTELLIGENCE_DEV.AI_CENTRE_FEATURE_STORE.PERSON_NEL_MASTER_INDEX pd
    ON yo.PERSON_ID = pd.PERSON_ID
GROUP BY
    yo.PERSON_ID,
    yo.OBSERVATION_YEAR,
    pd.GENDER,
    pd.ETHNIC_AIC_CATEGORY,
    pd.DATE_OF_BIRTH,
    pd.LONDON_IMD_RANK,
    pd.LONDON_IMD_DECILE,
    pd.IMD_QUINTILE,
    pd.IDACI_LONDON_RANK,
    pd.IDACI_LONDON_DECILE,
    pd.IDAOP_LONDON_RANK,
    pd.IDAOP_LONDON_DECILE,
    pd.PATIENT_STATUS,
    pd.INCLUDE_IN_LIST_SIZE_FLAG
"""

CREATE_CURRENT_TABLE_SQL = """
CREATE OR REPLACE TABLE INTELLIGENCE_DEV.AI_CENTRE_FEATURE_STORE.PERSON_5YEAR_PHENOTYPE AS
SELECT
    p.PERSON_ID,
    p.GENDER,
    p.ETHNIC_AIC_CATEGORY,
    p.DATE_OF_BIRTH,
    p.LONDON_IMD_RANK,
    p.LONDON_IMD_DECILE,
    p.IMD_QUINTILE,
    p.IDACI_LONDON_RANK,
    p.IDACI_LONDON_DECILE,
    p.IDAOP_LONDON_RANK,
    p.IDAOP_LONDON_DECILE,
    {phenotype_columns_window}
FROM INTELLIGENCE_DEV.AI_CENTRE_FEATURE_STORE.PERSON_PHENOTYPE_BY_YEAR p
WHERE p.OBSERVATION_YEAR >= (
    SELECT MAX(OBSERVATION_YEAR) - 5
    FROM INTELLIGENCE_DEV.AI_CENTRE_FEATURE_STORE.PERSON_PHENOTYPE_BY_YEAR
)
AND p.PATIENT_STATUS = 'ACTIVE'
AND p.INCLUDE_IN_LIST_SIZE_FLAG = 1
GROUP BY
    p.PERSON_ID,
    p.GENDER,
    p.ETHNIC_AIC_CATEGORY,
    p.DATE_OF_BIRTH,
    p.LONDON_IMD_RANK,
    p.LONDON_IMD_DECILE,
    p.IMD_QUINTILE,
    p.IDACI_LONDON_RANK,
    p.IDACI_LONDON_DECILE,
    p.IDAOP_LONDON_RANK,
    p.IDAOP_LONDON_DECILE
"""

CREATE_AGE_ONSET_TABLE_SQL = """
CREATE OR REPLACE TABLE INTELLIGENCE_DEV.AI_CENTRE_FEATURE_STORE.PERSON_PHENOTYPE_AGE_OF_ONSET AS
WITH phenotype_concepts AS (
    -- Get all relevant concept IDs for our phenotypes of interest
    SELECT DISTINCT
        PHENOTYPE_NAME,
        CORE_CONCEPT_ID
    FROM INTELLIGENCE_DEV.AI_CENTRE_PHENOTYPE_LIBRARY.PHENOSTORE
    WHERE (PHENOTYPE_SOURCE = 'LONDON' OR PHENOTYPE_SOURCE = 'ICB_NEL')
    AND PHENOTYPE_NAME IN ({phenotype_list})
    AND CORE_CONCEPT_ID IS NOT NULL
),
first_observations AS (
    -- Get first observation date for each person-phenotype combination
    SELECT
        o.PERSON_ID,
        --pc.PHENOTYPE_NAME,
        CASE pc.PHENOTYPE_NAME
            {phenotype_case_statement}
        END as PHENOTYPE_NAME,
        MIN(o.CLINICAL_EFFECTIVE_DATE) as FIRST_OBSERVATION_DATE,
        pd.DATE_OF_BIRTH,
        pd.GENDER,
        pd.ETHNIC_AIC_CATEGORY,
        pd.LONDON_IMD_RANK,
        pd.LONDON_IMD_DECILE,
        pd.IMD_QUINTILE,
        pd.IDACI_LONDON_RANK,
        pd.IDACI_LONDON_DECILE,
        pd.IDAOP_LONDON_RANK,
        pd.IDAOP_LONDON_DECILE
    FROM PROD_DWH.ANALYST_PRIMARY_CARE.OBSERVATION o
    INNER JOIN phenotype_concepts pc
        ON o.CORE_CONCEPT_ID = pc.CORE_CONCEPT_ID
    LEFT JOIN INTELLIGENCE_DEV.AI_CENTRE_FEATURE_STORE.PERSON_NEL_MASTER_INDEX pd
        ON o.PERSON_ID = pd.PERSON_ID
    WHERE o.CLINICAL_EFFECTIVE_DATE IS NOT NULL
    AND pd.DATE_OF_BIRTH IS NOT NULL
    GROUP BY
        o.PERSON_ID,
        pc.PHENOTYPE_NAME,
        pd.DATE_OF_BIRTH,
        pd.GENDER,
        pd.ETHNIC_AIC_CATEGORY,
        pd.LONDON_IMD_RANK,
        pd.LONDON_IMD_DECILE,
        pd.IMD_QUINTILE,
        pd.IDACI_LONDON_RANK,
        pd.IDACI_LONDON_DECILE,
        pd.IDAOP_LONDON_RANK,
        pd.IDAOP_LONDON_DECILE
)
SELECT
    PERSON_ID,
    PHENOTYPE_NAME,
    DATEDIFF(year, DATE_OF_BIRTH, FIRST_OBSERVATION_DATE) as AGE_AT_ONSET,
    FIRST_OBSERVATION_DATE,
    YEAR(FIRST_OBSERVATION_DATE) AS YEAR_OF_ONSET,
    DATE_OF_BIRTH,
    GENDER,
    ETHNIC_AIC_CATEGORY,
    LONDON_IMD_RANK,
    LONDON_IMD_DECILE,
    IMD_QUINTILE,
    IDACI_LONDON_RANK,
    IDACI_LONDON_DECILE,
    IDAOP_LONDON_RANK,
    IDAOP_LONDON_DECILE
FROM first_observations
"""

def load_phenotypes():
    """
    Loads phenotype configuration from JSON file
    Returns tuple of (column_names, phenotype_definitions)
    """
    with open("phenoconfig.json", "r") as f:
        pheno_dict = json.load(f)
    return list(pheno_dict.keys()), list(pheno_dict.values())

def generate_phenotype_columns(column_names, phenotype_definitions):
    """
    Generate SQL for pivoting phenotypes into columns
    Rename columns using imported dict
    """
    return ",\n    ".join([
        f"MAX(CASE WHEN PHENOTYPE_NAME = '{pheno_def}' THEN 1 ELSE 0 END) as {col_name}"
        for col_name, pheno_def in zip(column_names, phenotype_definitions)
    ])

def generate_phenotype_window_columns(column_names):
    """
    Generate the SQL for creating 5-year window columns
    """
    return ",\n    ".join([
        f"MAX({col_name}) as {col_name}"
        for col_name in column_names
    ])

def generate_phenotype_mapping_case(column_names, phenotype_definitions):
    """
    Generate CASE statement for mapping long phenotype names to short names
    """
    # Create case statements from the column names and definitions
    case_parts = []
    for short_name, long_name in zip(column_names, phenotype_definitions):
        case_parts.append(f"WHEN '{long_name}' THEN '{short_name}'")

    return "\n            ".join(case_parts)


def main():
    load_dotenv()
    try:
        snowsesh = SnowflakeConnection()
        snowsesh.use_database("INTELLIGENCE_DEV")
        snowsesh.use_schema("AI_CENTRE_FEATURE_STORE")

        # Load both column names and phenotype definitions
        COLUMN_NAMES, PHENOTYPES = load_phenotypes()

        phenotype_list_sql = ", ".join([f"'{p}'" for p in PHENOTYPES])
        phenotype_case_statement = generate_phenotype_mapping_case(COLUMN_NAMES, PHENOTYPES)

        phenotype_columns = generate_phenotype_columns(COLUMN_NAMES, PHENOTYPES)
        phenotype_window_columns = generate_phenotype_window_columns(COLUMN_NAMES)

        # create yearly table
        print("Creating PERSON_PHENOTYPE_BY_YEAR table...")
        yearly_sql = CREATE_YEARLY_TABLE_SQL.format(
            phenotype_list=phenotype_list_sql,
            phenotype_columns=phenotype_columns
        )
        snowsesh.execute_query(yearly_sql)

        # create current status table
        print("Creating PERSON_5YEAR_PHENOTYPE table...")
        current_sql = CREATE_CURRENT_TABLE_SQL.format(
            phenotype_columns_window=phenotype_window_columns
        )
        snowsesh.execute_query(current_sql)

        # create age of onset table with mapped names
        print("Creating PERSON_PHENOTYPE_AGE_OF_ONSET table...")
        onset_sql = CREATE_AGE_ONSET_TABLE_SQL.format(
            phenotype_list=phenotype_list_sql,
            phenotype_case_statement=phenotype_case_statement
        )
        snowsesh.execute_query(onset_sql)

        print("Feature store tables created successfully!")

    except Exception as e:
        print(f"Error creating feature store: {e}")
        raise e
    finally:
        snowsesh.session.close()

if __name__ == "__main__":
    main()