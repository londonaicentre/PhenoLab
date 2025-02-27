from dotenv import load_dotenv

from phmlondon.snow_utils import SnowflakeConnection

CREATE_DENOMINATOR_VIEW_SQL = """
CREATE OR REPLACE VIEW INTELLIGENCE_DEV.AI_CENTRE_FEATURE_STORE.DEMOGRAPHIC_DENOMINATOR_COUNT AS
WITH active_population AS (
    SELECT
        *,
        DATEDIFF(year, DATE_OF_BIRTH, CURRENT_DATE()) as current_age
    FROM INTELLIGENCE_DEV.AI_CENTRE_FEATURE_STORE.PERSON_NEL_MASTER_INDEX
    WHERE PATIENT_STATUS = 'ACTIVE'
    AND INCLUDE_IN_LIST_SIZE_FLAG = 1
),
total_count AS (
    SELECT COUNT(*) as total FROM active_population
),
age_bands AS (
    SELECT
        'age_band' as DEMOGRAPHIC_TYPE,
        CASE
            WHEN current_age < 18 THEN '0-17'
            WHEN current_age < 25 THEN '18-24'
            WHEN current_age < 35 THEN '25-34'
            WHEN current_age < 45 THEN '35-44'
            WHEN current_age < 55 THEN '45-54'
            WHEN current_age < 65 THEN '55-64'
            WHEN current_age < 75 THEN '65-74'
            ELSE '75+'
        END as DEMOGRAPHIC_VALUE,
        COUNT(*) as PERSON_COUNT
    FROM active_population
    GROUP BY DEMOGRAPHIC_VALUE
),
ethnicity_counts AS (
    SELECT
        'ethnicity' as DEMOGRAPHIC_TYPE,
        COALESCE(ETHNIC_AIC_CATEGORY, 'Unknown') as DEMOGRAPHIC_VALUE,
        COUNT(*) as PERSON_COUNT
    FROM active_population
    GROUP BY ETHNIC_AIC_CATEGORY
),
gender_counts AS (
    SELECT
        'gender' as DEMOGRAPHIC_TYPE,
        CASE
            WHEN GENDER = 'Male' THEN 'Male'
            WHEN GENDER = 'Female' THEN 'Female'
            ELSE 'Other'
        END as DEMOGRAPHIC_VALUE,
        COUNT(*) as PERSON_COUNT
    FROM active_population
    GROUP BY DEMOGRAPHIC_VALUE
),
imd_counts AS (
    SELECT
        'london_imd' as DEMOGRAPHIC_TYPE,
        CAST(IMD_QUINTILE as VARCHAR) as DEMOGRAPHIC_VALUE,
        COUNT(*) as PERSON_COUNT
    FROM active_population
    WHERE IMD_QUINTILE IS NOT NULL
    GROUP BY IMD_QUINTILE
),
total_population AS (
    SELECT
        'total_population' as DEMOGRAPHIC_TYPE,
        'total_population' as DEMOGRAPHIC_VALUE,
        COUNT(*) as PERSON_COUNT
    FROM active_population
),
combined_counts AS (
    SELECT * FROM total_population
    UNION ALL
    SELECT * FROM age_bands
    UNION ALL
    SELECT * FROM ethnicity_counts
    UNION ALL
    SELECT * FROM gender_counts
    UNION ALL
    SELECT * FROM imd_counts
)
SELECT
    c.*,
    ROUND(100.0 * c.PERSON_COUNT / (SELECT total FROM total_count), 2) as PERCENT_OF_TOTAL
FROM combined_counts c
ORDER BY --unsure why ordering isn't working
    CASE DEMOGRAPHIC_TYPE
        WHEN 'total_population' THEN 1
        WHEN 'age_band' THEN 2
        WHEN 'gender' THEN 3
        WHEN 'ethnicity' THEN 4
        WHEN 'london_imd' THEN 5
        ELSE 6
    END
"""


def main():
    """
    Creates a view of demographic denominators from the master person index
    """
    load_dotenv()

    try:
        snowsesh = SnowflakeConnection()
        snowsesh.use_database("INTELLIGENCE_DEV")
        snowsesh.use_schema("AI_CENTRE_FEATURE_STORE")

        snowsesh.execute_query(CREATE_DENOMINATOR_VIEW_SQL)

    except Exception as e:
        print(f"Error in main process: {e}")
        raise e
    finally:
        snowsesh.session.close()


if __name__ == "__main__":
    main()
