WITH hba1c_data AS (
    SELECT 
        PERSON_ID, 
        AGE_AT_EVENT, 
        CLINICAL_EFFECTIVE_DATE, 
        RESULT_VALUE_CLEANED_AND_CONVERTED_TO_MMOL_PER_MOL,
        ROW_NUMBER() OVER (PARTITION BY PERSON_ID ORDER BY CLINICAL_EFFECTIVE_DATE DESC) AS row_num_desc,
        ROW_NUMBER() OVER (PARTITION BY PERSON_ID ORDER BY CLINICAL_EFFECTIVE_DATE ASC) AS row_num_asc
    FROM INTELLIGENCE_DEV.AI_CENTRE_FEATURE_STORE.HBA1C_WITH_UNIT_REALLOCATION_V1
),
outcome_hba1c AS (
    SELECT 
        PERSON_ID,
        RESULT_VALUE_CLEANED_AND_CONVERTED_TO_MMOL_PER_MOL AS outcome_hba1c,
        CLINICAL_EFFECTIVE_DATE AS outcome_date,
        AGE_AT_EVENT AS outcome_age,
    FROM hba1c_data
    WHERE row_num_desc = 1
),
start_of_blinded_period AS (
    SELECT 
        PERSON_ID,
        DATEADD(YEAR, -1, outcome_date) AS start_of_blinded_period
    FROM outcome_hba1c
),
mean_hba1c AS (
    SELECT 
        h.PERSON_ID,
        ROUND(AVG(h.RESULT_VALUE_CLEANED_AND_CONVERTED_TO_MMOL_PER_MOL), 1) AS mean_hba1c
    FROM hba1c_data h
    LEFT JOIN start_of_blinded_period s ON h.PERSON_ID = s.PERSON_ID
    WHERE h.CLINICAL_EFFECTIVE_DATE < s.start_of_blinded_period
    GROUP BY h.PERSON_ID
),
median_hba1c AS (
    SELECT 
        h.PERSON_ID,
        ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY h.RESULT_VALUE_CLEANED_AND_CONVERTED_TO_MMOL_PER_MOL), 1) AS median_hba1c
    FROM hba1c_data h
    LEFT JOIN start_of_blinded_period s ON h.PERSON_ID = s.PERSON_ID
    WHERE h.CLINICAL_EFFECTIVE_DATE < s.start_of_blinded_period
    GROUP BY h.PERSON_ID
),
std_hba1c AS (
    SELECT 
        h.PERSON_ID,
        ROUND(STDDEV(h.RESULT_VALUE_CLEANED_AND_CONVERTED_TO_MMOL_PER_MOL), 2) AS std_hba1c
    FROM hba1c_data h
    LEFT JOIN start_of_blinded_period s ON h.PERSON_ID = s.PERSON_ID
    WHERE h.CLINICAL_EFFECTIVE_DATE < s.start_of_blinded_period
    GROUP BY h.PERSON_ID
),
last_hba1c_before_blinded_period AS (
    SELECT PERSON_ID, RESULT_VALUE_CLEANED_AND_CONVERTED_TO_MMOL_PER_MOL AS last_hba1c_before_blinded_period,
           CLINICAL_EFFECTIVE_DATE AS last_hba1c_date
    FROM (
        SELECT 
            h.PERSON_ID,
            h.RESULT_VALUE_CLEANED_AND_CONVERTED_TO_MMOL_PER_MOL,
            h.CLINICAL_EFFECTIVE_DATE,
            ROW_NUMBER() OVER (PARTITION BY h.PERSON_ID ORDER BY h.CLINICAL_EFFECTIVE_DATE DESC) AS rn
        FROM hba1c_data h
        LEFT JOIN start_of_blinded_period s ON h.PERSON_ID = s.PERSON_ID
        WHERE h.CLINICAL_EFFECTIVE_DATE < s.start_of_blinded_period
    )
    WHERE rn = 1
),
first_hba1c_on_record AS (
    SELECT PERSON_ID,
           RESULT_VALUE_CLEANED_AND_CONVERTED_TO_MMOL_PER_MOL AS first_hba1c_on_record,
           CLINICAL_EFFECTIVE_DATE AS first_hba1c_date
    FROM (
        SELECT 
            h.PERSON_ID,
            h.RESULT_VALUE_CLEANED_AND_CONVERTED_TO_MMOL_PER_MOL,
            h.CLINICAL_EFFECTIVE_DATE,
            ROW_NUMBER() OVER (PARTITION BY h.PERSON_ID ORDER BY h.CLINICAL_EFFECTIVE_DATE ASC) AS rn
        FROM hba1c_data h
        LEFT JOIN start_of_blinded_period s ON h.PERSON_ID = s.PERSON_ID
        WHERE h.CLINICAL_EFFECTIVE_DATE < s.start_of_blinded_period
    ) filtered
    WHERE rn = 1
),
slope_of_hba1c_over_time AS (
    SELECT 
        f.PERSON_ID,
        CASE 
            WHEN DATEDIFF(MONTH, f.first_hba1c_date, l.last_hba1c_date) = 0 THEN NULL
            ELSE (l.last_hba1c_before_blinded_period - f.first_hba1c_on_record) / 
                 DATEDIFF(MONTH, f.first_hba1c_date, l.last_hba1c_date)
        END AS slope_of_hba1c_over_time
    FROM first_hba1c_on_record f
    LEFT JOIN last_hba1c_before_blinded_period l ON f.PERSON_ID = l.PERSON_ID
),
num_hba1c_measurements AS (
    SELECT 
        h.PERSON_ID,
        COUNT(*) AS num_hba1c_measurements
    FROM hba1c_data h
    LEFT JOIN start_of_blinded_period s ON h.PERSON_ID = s.PERSON_ID
    WHERE h.CLINICAL_EFFECTIVE_DATE < s.start_of_blinded_period
    GROUP BY h.PERSON_ID
),
time_since_last_hba1c AS (
    SELECT 
        l.PERSON_ID,
        DATEDIFF(MONTH, s.start_of_blinded_period, l.last_hba1c_date) AS time_since_last_hba1c
    FROM last_hba1c_before_blinded_period l
    LEFT JOIN start_of_blinded_period s ON l.PERSON_ID = s.PERSON_ID
),
mean_hba1c_2_years_prior AS (
    SELECT 
        h.PERSON_ID,
        ROUND(AVG(h.RESULT_VALUE_CLEANED_AND_CONVERTED_TO_MMOL_PER_MOL), 1) AS mean_hba1c_2_years_prior
    FROM hba1c_data h
    LEFT JOIN start_of_blinded_period s ON h.PERSON_ID = s.PERSON_ID
    WHERE h.CLINICAL_EFFECTIVE_DATE BETWEEN DATEADD(YEAR, -2, s.start_of_blinded_period) AND s.start_of_blinded_period
    GROUP BY h.PERSON_ID
),
mean_hba1c_2_to_5_year_prior AS (
    SELECT 
        h.PERSON_ID,
        ROUND(AVG(h.RESULT_VALUE_CLEANED_AND_CONVERTED_TO_MMOL_PER_MOL), 1) AS mean_hba1c_2_to_5_year_prior
    FROM hba1c_data h
    LEFT JOIN start_of_blinded_period s ON h.PERSON_ID = s.PERSON_ID
    WHERE h.CLINICAL_EFFECTIVE_DATE BETWEEN DATEADD(YEAR, -5, s.start_of_blinded_period) AND DATEADD(YEAR, -2, s.start_of_blinded_period)
    GROUP BY h.PERSON_ID
),
age_at_first_hba1c_greater_or_equal_to_48 AS (
    SELECT 
        h.PERSON_ID,
        MIN(h.AGE_AT_EVENT) AS age_at_first_hba1c_greater_or_equal_to_48
    FROM hba1c_data h
    LEFT JOIN start_of_blinded_period s ON h.PERSON_ID = s.PERSON_ID
    WHERE h.RESULT_VALUE_CLEANED_AND_CONVERTED_TO_MMOL_PER_MOL >= 48
      AND h.CLINICAL_EFFECTIVE_DATE < s.start_of_blinded_period
    GROUP BY h.PERSON_ID
),
dob as (
    SELECT person_id, date_of_birth
    from INTELLIGENCE_DEV.AI_CENTRE_FEATURE_STORE.PERSON_MASTER_INDEX_V1
)
SELECT 
    o.PERSON_ID,
    o.outcome_hba1c,
    o.outcome_date,
    o.outcome_age,
    s.start_of_blinded_period,
    m.mean_hba1c,
    md.median_hba1c,
    sd.std_hba1c,
    l.last_hba1c_before_blinded_period,
    f.first_hba1c_on_record,
    sl.slope_of_hba1c_over_time,
    n.num_hba1c_measurements,
    t.time_since_last_hba1c,
    m2.mean_hba1c_2_years_prior,
    m2_5.mean_hba1c_2_to_5_year_prior,
    a.age_at_first_hba1c_greater_or_equal_to_48,
    ROUND(DATEDIFF(DAY, d.date_of_birth, s.start_of_blinded_period) / 365.25, 1) AS age_at_start_of_blinded_period
FROM outcome_hba1c o
LEFT JOIN start_of_blinded_period s ON o.PERSON_ID = s.PERSON_ID
LEFT JOIN mean_hba1c m ON o.PERSON_ID = m.PERSON_ID
LEFT JOIN median_hba1c md ON o.PERSON_ID = md.PERSON_ID
LEFT JOIN std_hba1c sd ON o.PERSON_ID = sd.PERSON_ID
LEFT JOIN last_hba1c_before_blinded_period l ON o.PERSON_ID = l.PERSON_ID
LEFT JOIN first_hba1c_on_record f ON o.PERSON_ID = f.PERSON_ID
LEFT JOIN slope_of_hba1c_over_time sl ON o.PERSON_ID = sl.PERSON_ID
LEFT JOIN num_hba1c_measurements n ON o.PERSON_ID = n.PERSON_ID
LEFT JOIN time_since_last_hba1c t ON o.PERSON_ID = t.PERSON_ID
LEFT JOIN mean_hba1c_2_years_prior m2 ON o.PERSON_ID = m2.PERSON_ID
LEFT JOIN mean_hba1c_2_to_5_year_prior m2_5 ON o.PERSON_ID = m2_5.PERSON_ID
LEFT JOIN age_at_first_hba1c_greater_or_equal_to_48 a ON o.PERSON_ID = a.PERSON_ID
LEFT JOIN dob d ON o.PERSON_ID = d.PERSON_ID;