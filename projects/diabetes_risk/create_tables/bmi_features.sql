WITH blinded_dates AS (
    SELECT person_id, start_of_blinded_period, outcome_date
    FROM INTELLIGENCE_DEV.AI_CENTRE_FEATURE_STORE.HBA1C_FEATURES_V1
),
bmi_filtered AS (
    SELECT b.*
    FROM INTELLIGENCE_DEV.AI_CENTRE_FEATURE_STORE.BMI_CLASSIFICATION_ALL_V1 b
    JOIN blinded_dates d ON b.person_id = d.person_id
    WHERE b.clinical_effective_date < d.start_of_blinded_period
),
mean_bmi AS (
    SELECT b.person_id, AVG(b.value) AS mean_bmi_in_obs_period
    FROM bmi_filtered as b
    JOIN blinded_dates d
    ON b.person_id = d.person_id
    WHERE b.CLINICAL_EFFECTIVE_DATE < d.start_of_blinded_period
    GROUP BY b.person_id
),
first_bmi AS (
    SELECT person_id, value AS first_bmi_in_obs_period, clinical_effective_date AS first_bmi_date
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY clinical_effective_date ASC, value ASC) AS rn
        FROM bmi_filtered
    )
    WHERE rn = 1
),
last_bmi AS (
    SELECT person_id, value AS last_bmi_in_obs_period, clinical_effective_date AS last_bmi_date
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY clinical_effective_date DESC, value DESC) AS rn
        FROM bmi_filtered
    )
    WHERE rn = 1
),
bmi_slope AS (
    SELECT 
        f. person_id, 
        CASE
            WHEN DATEDIFF(MONTH, f.first_bmi_date, l.last_bmi_date) = 0 THEN NULL
            ELSE (l.last_bmi_in_obs_period - f.first_bmi_in_obs_period) / DATEDIFF(MONTH, f.first_bmi_date, l.last_bmi_date)
        END AS slope_of_bmi_in_obs_period
    FROM first_bmi f
    JOIN last_bmi l
    on f.person_id = l.person_id
)
SELECT 
    b.person_id,
    m.mean_bmi_in_obs_period,
    l.last_bmi_in_obs_period,
    s.slope_of_bmi_in_obs_period
FROM blinded_dates as b
LEFT JOIN mean_bmi m on b.person_id = m.person_id
LEFT JOIN last_bmi l on b.person_id = l.person_id
LEFT JOIN bmi_slope s on b.person_id = s.person_id;