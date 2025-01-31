from dotenv import load_dotenv
from phmlondon.snow_utils import SnowflakeConnection
import pandas as pd

# UK Census-aligned ethnicity groupings
ETHNICITY_MAPPING = {
    'White': [
        'British',
        'Irish',
        'Any other White background'
    ],
    'Mixed': [
        'White and Black Caribbean',
        'White and Black African',
        'White and Asian',
        'Any other mixed background'
    ],
    'South Asian': [
        'Indian',
        'Pakistani',
        'Bangladeshi',
    ],
    'East or Other Asian': [
        'Chinese',
        'Any other Asian background'
    ],
    'Black': [
        'African',
        'Caribbean',
        'Any other Black background'
    ],
    'Other': [
        'Any other ethnic group'
    ],
    'Unknown': [
        'Not stated'
    ]
}

def create_ethnicity_mapping_case_statement():
    """
    Creates a CASE statement for mapping detailed ethnicity information to categories
    """
    case_parts = []
    for category, ethnicities in ETHNICITY_MAPPING.items():
        conditions = [f"ethnic_concept.DESCRIPTION = '{eth}'" for eth in ethnicities]
        case_parts.append(f"WHEN {' OR '.join(conditions)} THEN '{category}'")

    return f"""
    CASE
        {' '.join(case_parts)}
        ELSE 'Unknown'
    END as ETHNIC_CATEGORY
    """

def load_imd_data(snowsesh):
    """
    Loads imd2019london data from CSV into Snowflake table
    """
    try:
        df = pd.read_csv('data/imd2019london.csv')
        df.columns = [col.upper() for col in df.columns]

        snowsesh.load_dataframe_to_table(
            df=df,
            table_name='IMD2019LONDON',
            mode='overwrite'
        )
        print("IMD2019LONDON loaded successfully")
    except Exception as e:
        print(f"Error loading data into IMD2019LONDON: {e}")
        raise e

def create_person_demographics_view(snowsesh):
    try:
        ethnicity_case = create_ethnicity_mapping_case_statement()

        view_sql = f"""
        CREATE OR REPLACE VIEW INTELLIGENCE_DEV.AI_CENTRE_FEATURE_STORE.PERSON_DEMOGRAPHICS AS
        WITH ranked_patients AS (
            SELECT
                PERSON_ID,
                FIRST_VALUE(SK_PATIENTID) IGNORE NULLS OVER (PARTITION BY PERSON_ID ORDER BY ID DESC) as SK_PATIENTID,
                FIRST_VALUE(GENDER_CONCEPT_ID) IGNORE NULLS OVER (PARTITION BY PERSON_ID ORDER BY ID DESC) as GENDER_CONCEPT_ID,
                FIRST_VALUE(DATE_OF_BIRTH) IGNORE NULLS OVER (PARTITION BY PERSON_ID ORDER BY ID DESC) as DATE_OF_BIRTH,
                FIRST_VALUE(DATE_OF_DEATH) IGNORE NULLS OVER (PARTITION BY PERSON_ID ORDER BY ID DESC) as DATE_OF_DEATH,
                FIRST_VALUE(DATE_OF_DEATH_INC_CODES) IGNORE NULLS OVER (PARTITION BY PERSON_ID ORDER BY ID DESC) as DATE_OF_DEATH_INC_CODES,
                FIRST_VALUE(TYPE_1_OPT_OUT_FLAG) IGNORE NULLS OVER (PARTITION BY PERSON_ID ORDER BY ID DESC) as TYPE_1_OPT_OUT_FLAG,
                FIRST_VALUE(CURRENT_ADDRESS_ID) IGNORE NULLS OVER (PARTITION BY PERSON_ID ORDER BY ID DESC) as CURRENT_ADDRESS_ID,
                FIRST_VALUE(ETHNIC_CODE_CONCEPT_ID) IGNORE NULLS OVER (PARTITION BY PERSON_ID ORDER BY ID DESC) as ETHNIC_CODE_CONCEPT_ID,
                FIRST_VALUE(REGISTERED_PRACTICE_ORGANIZATION_ID) IGNORE NULLS OVER (PARTITION BY PERSON_ID ORDER BY ID DESC) as REGISTERED_PRACTICE_ORGANIZATION_ID,
                ROW_NUMBER() OVER (PARTITION BY PERSON_ID ORDER BY ID DESC) as rn
            FROM PROD_DWH.ANALYST_PRIMARY_CARE.PATIENT
        )
        SELECT
            p.*,
            gender_concept.DESCRIPTION as GENDER_DESCRIPTION,
            ethnic_concept.DESCRIPTION as ETHNICITY_DESCRIPTION,
            {ethnicity_case},
            addr.OUTCODE,
            addr.START_DATE,
            addr.END_DATE,
            addr.LSOA_2011_CODE,
            addr.MSOA_2011_CODE,
            addr.LOCAL_AUTHORITY_CODE,
            addr.ADDRESS_TYPE,
            -- IMD measures
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
            imd.EDU_LONDON_DECILE
        FROM ranked_patients p
        LEFT JOIN PROD_DWH.ANALYST_PRIMARY_CARE.PATIENT_ADDRESS addr
            ON p.CURRENT_ADDRESS_ID = addr.ID
        LEFT JOIN PROD_DWH.ANALYST_PRIMARY_CARE.CONCEPT gender_concept
            ON p.GENDER_CONCEPT_ID = gender_concept.DBID
        LEFT JOIN PROD_DWH.ANALYST_PRIMARY_CARE.CONCEPT ethnic_concept
            ON p.ETHNIC_CODE_CONCEPT_ID = ethnic_concept.DBID
        LEFT JOIN INTELLIGENCE_DEV.AI_CENTRE_FEATURE_STORE.IMD2019LONDON imd
            ON addr.LSOA_2011_CODE = imd.LS11CD
        WHERE p.rn = 1
        """

        snowsesh.execute_query(view_sql)
        print("Demographics view updated successfully, including IMD data")
    except Exception as e:
        print(f"Error updating demographics view: {e}")
        raise e

def main():
    load_dotenv()
    snowsesh = SnowflakeConnection()

    try:
        snowsesh.use_database("INTELLIGENCE_DEV")

        snowsesh.execute_query("""
        CREATE SCHEMA IF NOT EXISTS INTELLIGENCE_DEV.AI_CENTRE_FEATURE_STORE;
        """)
        snowsesh.use_schema("AI_CENTRE_FEATURE_STORE")

        load_imd_data(snowsesh)
        create_person_demographics_view(snowsesh)

        print("Person demographics view updated")

    except Exception as e:
        print(f"Error in main process: {e}")
        raise e
    finally:
        snowsesh.session.close()

if __name__ == "__main__":
    main()