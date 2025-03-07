-- Code for complaince work


CREATE OR REPLACE TABLE intelligence_dev.ai_centre_dev.drug_table_v3 AS
SELECT DISTINCT
    n.name AS order_name,
    m.code_description as drug,
    n.dbid AS concept_id,
    b.phenotype_name,
    m.codelist_name as class,
    n.scheme_name,
    n.dbid as core_concept_id
FROM intelligence_dev.ai_centre_phenotype_library.bnf_snomed_mappings b
LEFT JOIN prod_dwh.analyst_primary_care.concept n 
    ON n.code = b.code
LEFT JOIN intelligence_dev.ai_centre_phenotype_library.bsa_bnf_mappings m
    ON m.code = LEFT(b.codelist_id,9)

;
-- table of all orders of those with a compliance classification. 
-- with dates of orders, statements and complaince status

CREATE OR REPLACE TABLE intelligence_dev.ai_centre_dev.comp_orders AS
SELECT 
    o.id AS order_id,
    o.person_id,
    o.medication_statement_id,
    d.order_name as concept_name,
    c.name,
    d.drug,
    d.class,
    d.core_concept_id,
    o.core_concept_id as concept,
    o.clinical_effective_date AS order_date,
    o.duration_days,
    o.clinical_effective_date + o.duration_days AS order_enddate,
    s.clinical_effective_date as statement_date,
    s.cancellation_date as statement_enddate,
    mc.medication_compliance,
    mc.clinical_effective_date as compliance_date
FROM 
    prod_dwh.analyst_primary_care.medication_order o
LEFT JOIN 
    intelligence_dev.ai_centre_dev.drug_table_v3 d
    ON d.core_concept_id = o.core_concept_id
LEFT JOIN
    prod_dwh.analyst_primary_care.concept c
    ON c.dbid = o.core_concept_id
LEFT JOIN 
    prod_dwh.analyst_primary_care.medication_statement s
    ON s.id = o.medication_statement_id
-- Join the distinct medication compliance list
LEFT JOIN (
    SELECT DISTINCT person_id,
           CASE 
               WHEN SUM(CASE WHEN core_concept_id = '119686' THEN 1 ELSE 0 END) > 0
                    AND SUM(CASE WHEN core_concept_id = '239913' THEN 1 ELSE 0 END) > 0
               THEN 'both'
               WHEN SUM(CASE WHEN core_concept_id = '119686' THEN 1 ELSE 0 END) > 0
               THEN 'good'
               WHEN SUM(CASE WHEN core_concept_id = '239913' THEN 1 ELSE 0 END) > 0
               THEN 'poor'
               ELSE 'unknown'
           END AS medication_compliance
    FROM prod_dwh.analyst_primary_care.observation
    WHERE core_concept_id IN ('119686', '239913')
    GROUP BY person_id, compliance_date
) mc
    ON mc.person_id = o.person_id
-- Filter to only include people from the medication compliance list
WHERE mc.medication_compliance IS NOT NULL;

-- above table with additional analysis - days to next order and drug count
CREATE OR REPLACE TABLE intelligence_dev.ai_centre_dev.comp_analysis AS
WITH ranked_orders AS (
    SELECT 
        person_id,
        order_id,
        drug,
        class,
        order_date,
        order_enddate,
        duration_days,
        medication_statement_id,
        statement_date,
        statement_enddate,
        medication_compliance,
        compliance_date,
        ROW_NUMBER() OVER (PARTITION BY person_id, drug ORDER BY order_date) AS order_rank
    FROM intelligence_dev.ai_centre_dev.comp_orders
)
SELECT 
    a.person_id,
    a.order_id,
    a.medication_compliance,
    a.compliance_date,
    a.order_rank,
    a.drug,
    a.class,
    a.order_date,
    a.order_enddate,
    a.duration_days,
    CASE
    WHEN b.order_date IS NULL THEN NULL  -- No next order
    ELSE CASE 
        WHEN DATEDIFF(DAY, COALESCE(a.order_enddate, a.order_date), b.order_date) < 0 
        THEN 0 
        ELSE DATEDIFF(DAY, COALESCE(a.order_enddate, a.order_date), b.order_date) 
    END
END AS days_to_next_order, --this calculates the gaps between orders but doesnt count overlaps.
    a.medication_statement_id,
    a.statement_date,
    a.statement_enddate,
    COUNT(DISTINCT a.drug) OVER (PARTITION BY a.person_id) AS drug_count
FROM ranked_orders a
LEFT JOIN ranked_orders b
    ON a.person_id = b.person_id
    AND a.drug = b.drug
    AND a.order_rank = b.order_rank - 1
ORDER BY a.person_id, a.drug, a.order_rank
;


-- table to calcuate pdc based on duration of the order i.e if gap >duration, create new period
CREATE OR REPLACE TABLE intelligence_dev.ai_centre_dev.comp_pdc_duration AS
WITH ordered_orders AS (
    SELECT 
        person_id,
        order_rank,
        medication_compliance,
        compliance_date,
        drug,
        class,
        order_date,
        order_enddate,
        days_to_next_order,
        duration_days
    FROM intelligence_dev.ai_centre_dev.comp_analysis
),
periods AS (
    SELECT 
        person_id,
        order_rank,
        drug,
        class,
        medication_compliance,
        compliance_date,
        order_date,
        order_enddate,
        days_to_next_order,
        duration_days,
        -- Flag the next row if the gap between order_enddate and next_order_date exceeds the duration of the prescription
        CASE 
            WHEN days_to_next_order > duration_days THEN 1
            ELSE 0 
        END AS new_period_flag
    FROM ordered_orders
),
-- Shift the period flag to the next row using LAG() to calculate period_id properly
periods_with_shifted_flag AS (
    SELECT 
        person_id,
        order_rank,
        drug,
        class,
        medication_compliance,
        compliance_date,
        order_date,
        order_enddate,
        days_to_next_order,
        duration_days,
        -- Use LAG to shift the flag from current row to the next; for the first row, handle the NULL by setting it to 0
        COALESCE(LAG(new_period_flag) OVER (PARTITION BY person_id, drug ORDER BY order_rank), 0) AS shifted_new_period_flag
    FROM periods
),
periods_with_groups AS (
    SELECT 
        person_id,
        drug,
        class,
        medication_compliance,
        compliance_date,
        order_date,
        order_enddate,
        days_to_next_order,
        duration_days,
        shifted_new_period_flag,
        -- Calculate period_id by summing shifted_new_period_flag
        SUM(shifted_new_period_flag) OVER (PARTITION BY person_id, drug ORDER BY order_rank ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS period_id
    FROM periods_with_shifted_flag
)
SELECT 
    person_id,
    period_id + 1 AS period_id,  -- Add 1 to ensure period starts at 1
    drug,
    class,
    medication_compliance,
    compliance_date,
    MIN(order_date) AS period_start_date,
    MAX(order_enddate) AS period_end_date,
    DATEDIFF(DAY, period_start_date, period_end_date) AS duration_period,
    SUM(CASE 
            WHEN days_to_next_order <= duration_days THEN days_to_next_order 
            ELSE 0 
        END) AS order_gaps, 
    (duration_period - order_gaps) as duration_orders,
    CASE WHEN duration_orders = 0 OR duration_period = 0 THEN null
     ELSE duration_orders / duration_period
    END AS est_pdc,
    -- Rank the periods based on period_start_date for each person and drug
    RANK() OVER (PARTITION BY person_id ORDER BY MIN(order_date)) AS period_rank
FROM periods_with_groups
GROUP BY person_id, drug, period_id, medication_compliance, compliance_date, class
ORDER BY person_id, drug, period_id;


select count(*)
from intelligence_dev.ai_centre_dev.comp_pdc
where medication_compliance = 'both';

-- aggregated compliace socre per person
CREATE OR REPLACE TABLE intelligence_dev.ai_centre_dev.comp_person_pdc AS
SELECT 
    person_id,
    SUM(duration_period) AS total_duration,
    SUM(duration_orders) AS total_duration_with_orders,
    CASE 
        WHEN SUM(duration_period) = 0 THEN NULL  -- Avoid division by zero
        ELSE SUM(duration_orders) / SUM(duration_period)  -- Compliance calculation (PDC)
    END AS overall_compliance_score,
    medication_compliance
FROM intelligence_dev.ai_centre_dev.comp_pdc
GROUP BY person_id, medication_compliance;

SELECT 
    overall_compliance_score,
    medication_compliance
FROM 
    intelligence_dev.ai_centre_dev.comp_person_pdc
limit 10000;

-- only look at long term medication, top 3 chosen
CREATE OR REPLACE TABLE intelligence_dev.ai_centre_dev.comp_ltc_pdc AS
SELECT 
    person_id,
    SUM(duration_period) AS total_duration,
    SUM(duration_orders) AS total_duration_with_orders,
    CASE 
        WHEN SUM(duration_period) = 0 THEN NULL  -- Avoid division by zero
        ELSE SUM(duration_orders) / SUM(duration_period)  -- Compliance calculation (PDC)
    END AS overall_compliance_score,
    medication_compliance
FROM intelligence_dev.ai_centre_dev.comp_pdc_duration
WHERE class IN ('Lipid-regulating drugs', 'Calcium-channel blockers', 'Angiotensin-converting enzyme inhibitors')
GROUP BY person_id, medication_compliance
;