
--(2) HbA1cs windowed into yearly averages and then one value for the last 5 years
create table if not exists intelligence_dev.ai_centre_feature_store.t2dm_hba1c_binned as
with hba1cs as (
    select o.*, c.name
    from prod_dwh.analyst_primary_care.observation as o
    join prod_dwh.analyst_primary_care.concept as c
    on o.core_concept_id = c.dbid
    join intelligence_dev.ai_centre_phenotype_library.phenostore as ps
    on c.code = ps.code
    where ps.phenotype_id = '999023291000230101'
    -- and o.person_id = 51490
),
corrected_hba1cs as (
    select *,
        CASE
            WHEN result_value BETWEEN 4 AND 15 THEN '%'
            WHEN result_value BETWEEN 19 and 140 THEN 'mmol/mol'
            ELSE 'invalid'
        END AS result_value_inferred_units,
        CASE
            WHEN result_value BETWEEN 4 and 15 THEN ROUND((result_value*10.93) - 23.5, 2)
            WHEN result_value BETWEEN 19 and 140 THEN result_value
            ELSE NULL -- invalid values set to null
        END AS result_value_cleaned_and_converted_to_mmol_per_mol
    from hba1cs
), 
tidied_values as (
    select person_id, clinical_effective_date, age_at_event, result_value_cleaned_and_converted_to_mmol_per_mol 
    from corrected_hba1cs
    where result_value_cleaned_and_converted_to_mmol_per_mol is not null
),
final_date AS (
    SELECT
        person_id,
        MAX(clinical_effective_date) AS final_date,
    FROM tidied_values
    GROUP BY person_id
),
sorted_hba1c AS (
    SELECT 
        person_id,
        result_value_cleaned_and_converted_to_mmol_per_mol AS hba1c,
        ROW_NUMBER()
            OVER (PARTITION BY person_id ORDER BY clinical_effective_date DESC) AS row_num
    FROM tidied_values
),
final_hba1c AS (
    SELECT a.person_id, a.final_date, b.hba1c as final_hba1c
    FROM final_date AS a
    JOIN sorted_hba1c AS b
    ON a.person_id = b.person_id
    WHERE b.row_num = 1
),
yearly_hba1c AS (
    SELECT
        e.person_id,
        e.result_value_cleaned_and_converted_to_mmol_per_mol as hba1c,
        YEAR(e.clinical_effective_date) AS event_year
    FROM tidied_values e
    JOIN final_hba1c f
    ON e.person_id = f.person_id
    WHERE e.clinical_effective_date BETWEEN DATEADD(YEAR, -10, f.final_date) 
                          AND DATEADD(YEAR, -5, f.final_date)
),
yearly_avg AS (
    SELECT
        person_id,
        event_year,
        AVG(hba1c) AS avg_hba1c
    FROM yearly_hba1c
    GROUP BY person_id, event_year
),
enddate as (
    SELECT person_id, DATEADD (DAY, -5, f.final_date) AS date_at_end_of_observation_period
    FROM final_hba1c AS f
),
unique_dobs as (
    SELECT person_id, date_of_birth
    FROM prod_dwh.analyst_primary_care.patient
    GROUP BY person_id, date_of_birth
),
ages as (
    SELECT 
        a.person_id,
        DATEDIFF(year, b.date_of_birth, a.date_at_end_of_observation_period) as age_at_end_of_observation_period
    FROM enddate as a
    JOIN unique_dobs as b
    ON a.person_id = b.person_id
)
SELECT
    f.person_id,
    f.final_hba1c,
    COALESCE(MAX(CASE WHEN y.event_year = YEAR(f.final_date) - 10 THEN y.avg_hba1c END), NULL) AS hba1c_10_years_ago,
    COALESCE(MAX(CASE WHEN y.event_year = YEAR(f.final_date) - 9 THEN y.avg_hba1c END), NULL) AS hba1c_9_years_ago,
    COALESCE(MAX(CASE WHEN y.event_year = YEAR(f.final_date) - 8 THEN y.avg_hba1c END), NULL) AS hba1c_8_years_ago,
    COALESCE(MAX(CASE WHEN y.event_year = YEAR(f.final_date) - 7 THEN y.avg_hba1c END), NULL) AS hba1c_7_years_ago,
    COALESCE(MAX(CASE WHEN y.event_year = YEAR(f.final_date) - 6 THEN y.avg_hba1c END), NULL) AS hba1c_6_years_ago,
    a.age_at_end_of_observation_period
FROM final_hba1c f
LEFT JOIN yearly_avg y
ON f.person_id = y.person_id
JOIN ages a
ON y.person_id = a.person_id
GROUP BY f.person_id, f.final_hba1c, a.age_at_end_of_observation_period
ORDER BY f.person_id;
