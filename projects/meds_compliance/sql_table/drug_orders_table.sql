--SQL to make a table that joins the orders, statements and drug table labels
--does not filter out orders which will be missing a join with the drug table due to missing core_concept_id
-- there is a calculated field - the days to next orders field which is the difference between the current orderdate and the next order date

CREATE OR REPLACE TABLE intelligence_dev.ai_centre_dev.drug_orders AS
WITH base_data AS (
    SELECT 
        o.id AS order_id,
        o.person_id,
        o.medication_statement_id,
        d.drug_name,
        o.dose,
        o.quantity_value,
        o.quantity_unit,
        d.drug_description,
        d.class,
        d.core_concept_id,
        o.non_core_concept_id,
        o.clinical_effective_date AS order_date,
        o.duration_days,
        s.clinical_effective_date AS statement_date,
        s.cancellation_date AS statement_enddate
    FROM 
        prod_dwh.analyst_primary_care.medication_order o
    LEFT JOIN 
        prod_dwh.analyst_primary_care.medication_statement s
        ON s.id = o.medication_statement_id
    LEFT JOIN 
        intelligence_dev.ai_centre_dev.drug_table_v4 d
        ON d.core_concept_id = COALESCE(o.core_concept_id, s.core_concept_id, o.non_core_concept_id)
),
ordered_with_next AS (
    SELECT *,
        LEAD(order_date) OVER (
            PARTITION BY person_id, drug_name
            ORDER BY order_date
        ) AS next_order_date
    FROM base_data
)
SELECT *,
    DATEDIFF('day', order_date, next_order_date) AS days_to_next_order
FROM ordered_with_next
;