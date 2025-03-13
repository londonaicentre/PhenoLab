--(1) total adult population
SELECT COUNT(DISTINCT PERSON_ID)
FROM PATIENT
WHERE APPROX_CURRENT_AGE >= 18;

-- (2) total adult population with hypertension
SELECT COUNT(DISTINCT p.person_id)
FROM patient as p
JOIN PROD_DWH.ANALYST_PRIMARY_CARE.OBSERVATION AS obs
    ON obs.person_id = p.person_id
JOIN PROD_DWH.ANALYST_PRIMARY_CARE.CONCEPT AS concept
    ON obs.core_concept_id = concept.dbid
JOIN INTELLIGENCE_DEV.AI_CENTRE_PHENOTYPE_LIBRARY.PHENOSTORE AS pheno
    ON concept.CODE = pheno.code
WHERE pheno.PHENOTYPE_ID = 'opensafely/hypertension-snomed'
AND p.approx_current_age >=18;

-- (3) at risk

-- (4) total adult population with stroke
SELECT COUNT(DISTINCT p.person_id)
FROM patient as p
JOIN PROD_DWH.ANALYST_PRIMARY_CARE.OBSERVATION AS obs
    ON obs.person_id = p.person_id
JOIN PROD_DWH.ANALYST_PRIMARY_CARE.CONCEPT AS concept
    ON obs.core_concept_id = concept.dbid
JOIN INTELLIGENCE_DEV.AI_CENTRE_PHENOTYPE_LIBRARY.PHENOSTORE AS pheno
    ON concept.CODE = pheno.code
WHERE pheno.PHENOTYPE_ID = 'opensafely/stroke-snomed'
AND p.approx_current_age >=18;

-- (5) total adult population with CKD
SELECT COUNT(DISTINCT p.person_id)
FROM patient as p
JOIN PROD_DWH.ANALYST_PRIMARY_CARE.OBSERVATION AS obs
    ON obs.person_id = p.person_id
JOIN PROD_DWH.ANALYST_PRIMARY_CARE.CONCEPT AS concept
    ON obs.core_concept_id = concept.dbid
JOIN INTELLIGENCE_DEV.AI_CENTRE_PHENOTYPE_LIBRARY.PHENOSTORE AS pheno
    ON concept.CODE = pheno.code
WHERE pheno.PHENOTYPE_ID = 'CHRONIC KIDNEY DISEASE'
AND p.approx_current_age >=18;


-- (6) total adult population with IHD
SELECT COUNT(DISTINCT p.person_id)
FROM patient as p
JOIN PROD_DWH.ANALYST_PRIMARY_CARE.OBSERVATION AS obs
    ON obs.person_id = p.person_id
JOIN PROD_DWH.ANALYST_PRIMARY_CARE.CONCEPT AS concept
    ON obs.core_concept_id = concept.dbid
JOIN INTELLIGENCE_DEV.AI_CENTRE_PHENOTYPE_LIBRARY.PHENOSTORE AS pheno
    ON concept.CODE = pheno.code
WHERE pheno.PHENOTYPE_ID = 'CORONARY HEART DISEASE'
AND p.approx_current_age >=18;

