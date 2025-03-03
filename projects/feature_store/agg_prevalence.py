import json

from dotenv import load_dotenv

from phmlondon.snow_utils import SnowflakeConnection

# Generates wide table of aggregate demographic prevalence
# Requires deographic demoniators to be generated first

PHENOTYPE_DEMOGRAPHIC_SQL = """
CREATE OR REPLACE TABLE INTELLIGENCE_DEV.AI_CENTRE_FEATURE_STORE.PHENOTYPE_DEMOGRAPHIC_PREVALENCE AS
WITH demographic_groups AS (
    SELECT
        PERSON_ID,
        CASE
            WHEN GENDER IN ('Male', 'Female') THEN GENDER
            ELSE 'Other'
        END as GENDER_GROUP,
        CASE
            WHEN DATEDIFF(year, DATE_OF_BIRTH, CURRENT_DATE()) < 18 THEN '0-17'
            WHEN DATEDIFF(year, DATE_OF_BIRTH, CURRENT_DATE()) < 25 THEN '18-24'
            WHEN DATEDIFF(year, DATE_OF_BIRTH, CURRENT_DATE()) < 35 THEN '25-34'
            WHEN DATEDIFF(year, DATE_OF_BIRTH, CURRENT_DATE()) < 45 THEN '35-44'
            WHEN DATEDIFF(year, DATE_OF_BIRTH, CURRENT_DATE()) < 55 THEN '45-54'
            WHEN DATEDIFF(year, DATE_OF_BIRTH, CURRENT_DATE()) < 65 THEN '55-64'
            WHEN DATEDIFF(year, DATE_OF_BIRTH, CURRENT_DATE()) < 75 THEN '65-74'
            ELSE '75+'
        END as AGE_GROUP,
        ETHNIC_AIC_CATEGORY as ETHNIC_GROUP,
        CAST(IMD_QUINTILE as VARCHAR) as IMD_GROUP,
        {phenotype_columns}
    FROM INTELLIGENCE_DEV.AI_CENTRE_FEATURE_STORE.PERSON_5YEAR_PHENOTYPE
),
gender_counts AS (
    SELECT
        'gender' as DEMOGRAPHIC_TYPE,
        GENDER_GROUP as DEMOGRAPHIC_VALUE,
        {phenotype_sums}
    FROM demographic_groups
    GROUP BY GENDER_GROUP
),
age_counts AS (
    SELECT
        'age_band' as DEMOGRAPHIC_TYPE,
        AGE_GROUP as DEMOGRAPHIC_VALUE,
        {phenotype_sums}
    FROM demographic_groups
    GROUP BY AGE_GROUP
),
ethnicity_counts AS (
    SELECT
        'ethnicity' as DEMOGRAPHIC_TYPE,
        ETHNIC_GROUP as DEMOGRAPHIC_VALUE,
        {phenotype_sums}
    FROM demographic_groups
    GROUP BY ETHNIC_GROUP
),
imd_counts AS (
    SELECT
        'london_imd' as DEMOGRAPHIC_TYPE,
        IMD_GROUP as DEMOGRAPHIC_VALUE,
        {phenotype_sums}
    FROM demographic_groups
    WHERE IMD_GROUP IS NOT NULL
    GROUP BY IMD_GROUP
),
all_counts AS (
    SELECT * FROM gender_counts
    UNION ALL
    SELECT * FROM age_counts
    UNION ALL
    SELECT * FROM ethnicity_counts
    UNION ALL
    SELECT * FROM imd_counts
)
SELECT
    ac.*,
    dd.PERSON_COUNT as DEMOGRAPHIC_DENOMINATOR,
    {prevalence_calculations}
    -- {', '.join([
    --     f'ROUND(100.0 * "{p}_COUNT" / dd.PERSON_COUNT, 2) as "{p}_PREVALENCE"'
    --     for p in PHENOTYPES
    -- ])}
FROM all_counts ac
LEFT JOIN INTELLIGENCE_DEV.AI_CENTRE_FEATURE_STORE.DEMOGRAPHIC_DENOMINATOR_COUNT dd
    ON ac.DEMOGRAPHIC_TYPE = dd.DEMOGRAPHIC_TYPE
    AND ac.DEMOGRAPHIC_VALUE = dd.DEMOGRAPHIC_VALUE
ORDER BY
    CASE DEMOGRAPHIC_TYPE
        WHEN 'gender' THEN 1
        WHEN 'age_band' THEN 2
        WHEN 'ethnicity' THEN 3
        WHEN 'london_imd' THEN 4
        ELSE 5
    END
"""


def load_phenotypes():
    """
    Loads phenotype configuration from JSON file
    Returns list of column names
    """
    with open("phenoconfig.json", "r") as f:
        pheno_dict = json.load(f)
    return list(pheno_dict.keys())


# def get_demographic_denominators(snowsesh):
#     """
#     Retrieves demographic denominators from the denominator view
#     Returns nested dictionary of demographic type -> value -> count
#     **Refactored into main query**
#     """
#     query = """
#     SELECT
#         DEMOGRAPHIC_TYPE,
#         DEMOGRAPHIC_VALUE,
#         PERSON_COUNT
#     FROM INTELLIGENCE_DEV.AI_CENTRE_FEATURE_STORE.DEMOGRAPHIC_DENOMINATOR_COUNT
#     """

#     df = snowsesh.execute_query_to_df(query)

#     # convert to nested dictionary
#     denominators = {}
#     for _, row in df.iterrows():
#         demo_type = row['DEMOGRAPHIC_TYPE']
#         if demo_type not in denominators:
#             denominators[demo_type] = {}
#         denominators[demo_type][row['DEMOGRAPHIC_VALUE']] = row['PERSON_COUNT']

#     return denominators


def generate_phenotype_sum_columns(phenotypes):
    """
    Generates SQL for summing phenotype columns
    """
    return ",\n".join([f'SUM({phenotype}) as "{phenotype}_COUNT"' for phenotype in phenotypes])


def create_demographic_prevalence_table(snowsesh):
    """
    Creates table containing demographic prevalence calculations
    """
    PHENOTYPES = load_phenotypes()

    # prepare dynamic parts
    phenotype_sums = generate_phenotype_sum_columns(PHENOTYPES)
    phenotype_columns = ", ".join([f'"{p}"' for p in PHENOTYPES])

    prevalence_calculations = ", ".join(
        [f'ROUND(100.0 * "{p}_COUNT" / dd.PERSON_COUNT, 2) as "{p}_PREVALENCE"' for p in PHENOTYPES]
    )

    print(phenotype_sums)
    print(phenotype_columns)
    print(prevalence_calculations)  # Debugging: see the generated string

    sql = (
        PHENOTYPE_DEMOGRAPHIC_SQL.replace("{phenotype_columns}", phenotype_columns)
        .replace("{phenotype_sums}", phenotype_sums)
        .replace("{prevalence_calculations}", prevalence_calculations)
    )

    snowsesh.execute_query(sql)
    print("Created phenotype prevalence table")


def main():
    """
    Creates a table of phenotype prevalence by demographic group
    """
    load_dotenv()

    try:
        snowsesh = SnowflakeConnection()
        snowsesh.use_database("INTELLIGENCE_DEV")
        snowsesh.use_schema("AI_CENTRE_FEATURE_STORE")

        create_demographic_prevalence_table(snowsesh)

        print("Phenotype prevalence analysis complete!")

    except Exception as e:
        print(f"Error in prevalence analysis: {e}")
        raise e
    finally:
        snowsesh.session.close()


if __name__ == "__main__":
    main()
