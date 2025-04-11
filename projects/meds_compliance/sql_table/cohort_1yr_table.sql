
-- choose the top 7 classes after filtering the nulls. (checked this and is plausible)
CREATE OR REPLACE TABLE intelligence_dev.ai_centre_dev.compliance_cohort_1yr_top7 AS
SELECT *
FROM intelligence_dev.ai_centre_dev.compliance_cohort_1yr
WHERE class IN (
    SELECT class
    FROM intelligence_dev.ai_centre_dev.compliance_cohort_1yr
    WHERE class IS NOT NULL
      AND class != 'Non-opioid analgesics and compound preparations'
    GROUP BY class
    ORDER BY COUNT(DISTINCT person_id) DESC
    LIMIT 7
  );



-- an unfiltered cohort where coliance status is matched to +- 1yr of drug orders
-- includes those wth no core_concept_id and any drug!!
--needs fltering/exploring

CREATE OR REPLACE TABLE intelligence_dev.ai_centre_dev.compliance_cohort_1yr AS
SELECT 
        do.order_id,
        do.person_id,
        do.medication_statement_id,
        do.drug_name,
        do.dose,
        do.quantity_value,
        do.quantity_unit,
        do.drug_description,
        do.class,
        do.core_concept_id,
        do.order_date,
        do.duration_days,
        do.days_to_next_order,
        do.statement_date,
        do.statement_enddate,
        cs.medication_compliance
FROM
   intelligence_dev.ai_centre_dev.compliance_status cs
JOIN
    intelligence_dev.ai_centre_dev.drug_orders do
    ON cs.person_id = do.person_id
    AND do.order_date BETWEEN DATEADD(year, -1, cs.clinical_effective_date)
                          AND DATEADD(year, 1, cs.clinical_effective_date)
WHERE
    cs.medication_compliance IN ('good', 'poor')
;