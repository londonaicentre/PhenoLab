import pandas as pd
from dotenv import load_dotenv

from phmlondon.snow_utils import SnowflakeConnection
from phmlondon.feature_store_manager import FeatureStoreManager

from phmlondon.config import SNOWFLAKE_DATABASE, EXTERNAL, FEATURE_STORE, FEATURE_METADATA

# Generates BASE_PERSON_NEL_INDEX
# Required table for downstream feature store creation
# Contains basic sociodemographic and spatioeconomic data for each unique PERSON
# This contains the following two classes of PERSON:
# (1) Currently alive, registered, and resident in NEL
# (2) Any historical patient previously registered and resident in NEL, but now either dead or de-registered

# UK Census-aligned ethnicity groupings with addition of East Asian
ETHNICITY_MAPPING = {
    "White": ["British", "Irish", "Any Other White Background"],
    "Mixed": [
        "White And Black Caribbean",
        "White And Black African",
        "White And Asian",
        "Any Other Mixed Background",
    ],
    "South Asian": [
        "Indian",
        "Pakistani",
        "Bangladeshi",
    ],
    "East Or Other Asian": ["Chinese", "Any Other Asian Background"],
    "Black": ["African", "Caribbean", "Any Other Black Background"],
    "Other": ["Any Other Ethnic Group"],
    "Not Stated": ["Not Stated"],
    "Unknown": ["Not Known"],
}

CREATE_MASTER_INDEX_SQL = """
WITH PatientHistory AS (
    SELECT
        PERSON_ID,
        START_OF_MONTH,
        END_OF_MONTH,
        PRACTICE_CODE,
        PRACTICE_NAME,
        REGISTRATION_START_DATE,
        REGISTRATION_END_DATE,
        INCLUDE_IN_LIST_SIZE_FLAG,
        GENDER,
        ETHNICITY,
        ETHNICITY_DETAIL,
        ETHNICITY_MAIN_CATEGORY,
        {ethnicity_case},
        PATIENT_LSOA_2011,
        PATIENT_LSOA_2021,
        IMD_DECILE,
        IMD_QUINTILE,
        DATE_OF_DEATH AS DATE_OF_DEATH_PMI,
        DATE_OF_DEATH_DATASET,
        UPRN_SUGGESTS_LIVES_ALONE_FLAG,
        SMOKING_STATUS,
        LATEST_SMOKING_STATUS_DATE,
        EVER_HOMELESS_FLAG,
        HOUSEBOUND_FLAG,
        HOUSEBOUND_TYPE,
        LATEST_HOUSEBOUND_CODE_DATE,
        ROW_NUMBER() OVER (PARTITION BY PERSON_ID ORDER BY END_OF_MONTH DESC) as rn
    FROM PROD_DWH.ANALYST_FACTS.PMI
    WHERE REGISTERED_IN_NEL_FLAG = 1
    AND RESIDENT_IN_NEL_FLAG = 1
),
LatestPatientRecords AS (
    SELECT *
    FROM PatientHistory
    QUALIFY rn = 1
),
DateOfBirth AS (
    SELECT DISTINCT
        PERSON_ID,
        FIRST_VALUE(DATE_OF_BIRTH) IGNORE NULLS OVER (PARTITION BY PERSON_ID ORDER BY ID DESC) as DATE_OF_BIRTH,
        FIRST_VALUE(DATE_OF_DEATH) IGNORE NULLS OVER (PARTITION BY PERSON_ID ORDER BY ID DESC) as DATE_OF_DEATH_B
    FROM PROD_DWH.ANALYST_PRIMARY_CARE.PATIENT
)
SELECT
    lpr.*,
    COALESCE(lpr.DATE_OF_DEATH_PMI, dob.DATE_OF_DEATH_B) AS DATE_OF_DEATH,
    dob.DATE_OF_BIRTH,
    imd.LONDON_IMD_RANK,
    imd.LONDON_IMD_DECILE,
    imd.IDAOP_LONDON_RANK,
    imd.IDAOP_LONDON_DECILE,
    imd.IDACI_LONDON_RANK,
    imd.IDACI_LONDON_DECILE,
    imd.EMPLOYMENT_LONDON_RANK,
    imd.EMPLOYMENT_LONDON_DECILE,
    imd.INCOME_LONDON_RANK,
    imd.INCOME_LONDON_DECILE,
    imd.EDU_LONDON_RANK,
    imd.EDU_LONDON_DECILE,
    CASE
        WHEN DATE_OF_DEATH IS NOT NULL THEN 'DEATH'
        WHEN REGISTRATION_END_DATE < DATEADD(month, -1, CURRENT_DATE()) THEN 'DEREGISTERED'
        ELSE 'ACTIVE'
    END AS PATIENT_STATUS
FROM LatestPatientRecords lpr
LEFT JOIN {database}.{external}.IMD2019LONDON imd
ON lpr.PATIENT_LSOA_2011 = imd.LS11CD
INNER JOIN DateOfBirth dob --inner join to keep the primary care patient table as a main source of truth
ON lpr.PERSON_ID = dob.PERSON_ID
ORDER BY lpr.START_OF_MONTH DESC;
"""


def create_ethnicity_mapping_case_statement():
    """
    Creates a CASE statement for mapping detailed ethnicity information to categories
    """
    case_parts = []
    for category, ethnicities in ETHNICITY_MAPPING.items():
        conditions = [f"'{eth}'" for eth in ethnicities]
        case_parts.append(f"WHEN ETHNICITY IN ({', '.join(conditions)}) THEN '{category}'")

    return f"""
    CASE
        {" ".join(case_parts)}
        ELSE 'MISSING'
    END as ETHNIC_AIC_CATEGORY
    """


def load_imd_data(snowsesh):
    """
    Loads imd2019london data from CSV into Snowflake table
    """
    try:
        df = pd.read_csv("data/imd2019london.csv")
        df.columns = [col.upper() for col in df.columns]

        snowsesh.use_database(SNOWFLAKE_DATABASE)
        snowsesh.create_schema_if_not_exists(EXTERNAL)
        snowsesh.use_schema(EXTERNAL)

        snowsesh.load_dataframe_to_table(df=df, table_name="IMD2019LONDON", mode="overwrite")
        print("IMD2019LONDON loaded successfully")
    except Exception as e:
        print(f"Error loading data into IMD2019LONDON: {e}")
        raise e


def create_base_person_nel_index_table(snowsesh, fsm):
    ethnicity_case = create_ethnicity_mapping_case_statement()
    master_person_sql = CREATE_MASTER_INDEX_SQL.format(
        ethnicity_case=ethnicity_case,
        database=SNOWFLAKE_DATABASE,
        external=EXTERNAL)
    fsm.add_new_feature(
        feature_name="Base Person NEL Index",
        feature_desc="""
            Base Person Master Index table for NEL that is necessary for subsequent features to build.
            It contains sociodemographic and spatioeconomic information per patient.
        """,
        feature_format="Wide, Mixed",
        sql_select_query_to_generate_feature=master_person_sql,
        existence_ok=True,
    )


def main():
    load_dotenv()

    try:
        snowsesh = SnowflakeConnection()

        load_imd_data(snowsesh)

        snowsesh.use_database(SNOWFLAKE_DATABASE)
        snowsesh.use_schema(FEATURE_STORE)

        fsm = FeatureStoreManager(snowsesh, SNOWFLAKE_DATABASE, FEATURE_STORE, FEATURE_METADATA)
        print("Feature store manager created")

        create_base_person_nel_index_table(snowsesh, fsm)
        print("base person index table updated")

    except Exception as e:
        print(f"Error creating feature store: {e}")
        raise e
    finally:
        snowsesh.session.close()


if __name__ == "__main__":
    main()
