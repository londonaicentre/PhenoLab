CREATE OR REPLACE TABLE intelligence_dev.ai_centre_dev.compliance_status AS
SELECT DISTINCT person_id,
    clinical_effective_date,
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
    GROUP BY person_id, clinical_effective_date;