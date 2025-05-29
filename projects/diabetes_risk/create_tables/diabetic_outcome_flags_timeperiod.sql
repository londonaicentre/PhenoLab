WITH blinded_dates AS (
    SELECT person_id, start_of_blinded_period, outcome_date
    FROM INTELLIGENCE_DEV.AI_CENTRE_FEATURE_STORE.HBA1C_FEATURES_V1
), 
diabetic_eye_complications_pre AS (
    SELECT DISTINCT o.person_id
    FROM PROD_DWH.ANALYST_PRIMARY_CARE.OBSERVATION o
    JOIN PROD_DWH.ANALYST_PRIMARY_CARE.CONCEPT c ON o.core_concept_id = c.dbid
    JOIN INTELLIGENCE_DEV.AI_CENTRE_DEFINITION_LIBRARY.DEFINITIONSTORE d ON c.code = d.code
    JOIN blinded_dates b ON b.person_id = o.person_id
    WHERE d.definition_name = 'diabetic_eye_complications'
      AND o.clinical_effective_date < b.start_of_blinded_period
),
diabetic_eye_complications_post AS (
    SELECT DISTINCT o.person_id
    FROM PROD_DWH.ANALYST_PRIMARY_CARE.OBSERVATION o
    JOIN PROD_DWH.ANALYST_PRIMARY_CARE.CONCEPT c ON o.core_concept_id = c.dbid
    JOIN INTELLIGENCE_DEV.AI_CENTRE_DEFINITION_LIBRARY.DEFINITIONSTORE d ON c.code = d.code
    JOIN blinded_dates b ON b.person_id = o.person_id
    WHERE d.definition_name = 'diabetic_eye_complications'
      AND o.clinical_effective_date >= b.start_of_blinded_period
      AND o.clinical_effective_date < b.outcome_date
),
hf_pre AS (
    SELECT DISTINCT o.person_id
    FROM PROD_DWH.ANALYST_PRIMARY_CARE.OBSERVATION o
    JOIN PROD_DWH.ANALYST_PRIMARY_CARE.CONCEPT c ON o.core_concept_id = c.dbid
    JOIN INTELLIGENCE_DEV.AI_CENTRE_DEFINITION_LIBRARY.DEFINITIONSTORE d ON c.code = d.code
    JOIN blinded_dates b ON b.person_id = o.person_id
    WHERE d.definition_name = 'heart_failure_cause_not_specified_or_due_to_CAD'
      AND o.clinical_effective_date < b.start_of_blinded_period
),
hf_post AS (
    SELECT DISTINCT o.person_id
    FROM PROD_DWH.ANALYST_PRIMARY_CARE.OBSERVATION o
    JOIN PROD_DWH.ANALYST_PRIMARY_CARE.CONCEPT c ON o.core_concept_id = c.dbid
    JOIN INTELLIGENCE_DEV.AI_CENTRE_DEFINITION_LIBRARY.DEFINITIONSTORE d ON c.code = d.code
    JOIN blinded_dates b ON b.person_id = o.person_id
    WHERE d.definition_name = 'heart_failure_cause_not_specified_or_due_to_CAD'
      AND o.clinical_effective_date >= b.start_of_blinded_period
      AND o.clinical_effective_date < b.outcome_date
),
neuropathy_pre AS (
    SELECT DISTINCT o.person_id
    FROM PROD_DWH.ANALYST_PRIMARY_CARE.OBSERVATION o
    JOIN PROD_DWH.ANALYST_PRIMARY_CARE.CONCEPT c ON o.core_concept_id = c.dbid
    JOIN INTELLIGENCE_DEV.AI_CENTRE_DEFINITION_LIBRARY.DEFINITIONSTORE d ON c.code = d.code
    JOIN blinded_dates b ON b.person_id = o.person_id
    WHERE d.definition_name = 'neuropathy_due_to_nonT1DM_peripheral_autonomic_or_gastroparesis_894add3f.json'
      AND o.clinical_effective_date < b.start_of_blinded_period
),
neuropathy_post AS (
    SELECT DISTINCT o.person_id
    FROM PROD_DWH.ANALYST_PRIMARY_CARE.OBSERVATION o
    JOIN PROD_DWH.ANALYST_PRIMARY_CARE.CONCEPT c ON o.core_concept_id = c.dbid
    JOIN INTELLIGENCE_DEV.AI_CENTRE_DEFINITION_LIBRARY.DEFINITIONSTORE d ON c.code = d.code
    JOIN blinded_dates b ON b.person_id = o.person_id
    WHERE d.definition_name = 'neuropathy_due_to_nonT1DM_peripheral_autonomic_or_gastroparesis_894add3f.json'
      AND o.clinical_effective_date >= b.start_of_blinded_period
      AND o.clinical_effective_date < b.outcome_date
),
amputation_pre AS (
    SELECT DISTINCT o.person_id
    FROM PROD_DWH.ANALYST_PRIMARY_CARE.OBSERVATION o
    JOIN PROD_DWH.ANALYST_PRIMARY_CARE.CONCEPT c ON o.core_concept_id = c.dbid
    JOIN INTELLIGENCE_DEV.AI_CENTRE_DEFINITION_LIBRARY.DEFINITIONSTORE d ON c.code = d.code
    JOIN blinded_dates b ON b.person_id = o.person_id
    WHERE d.definition_name = 'amputation_in_lower_limb_nontraumatic'
      AND o.clinical_effective_date < b.start_of_blinded_period
),
amputation_post AS (
    SELECT DISTINCT o.person_id
    FROM PROD_DWH.ANALYST_PRIMARY_CARE.OBSERVATION o
    JOIN PROD_DWH.ANALYST_PRIMARY_CARE.CONCEPT c ON o.core_concept_id = c.dbid
    JOIN INTELLIGENCE_DEV.AI_CENTRE_DEFINITION_LIBRARY.DEFINITIONSTORE d ON c.code = d.code
    JOIN blinded_dates b ON b.person_id = o.person_id
    WHERE d.definition_name = 'amputation_in_lower_limb_nontraumatic'
      AND o.clinical_effective_date >= b.start_of_blinded_period
      AND o.clinical_effective_date < b.outcome_date
),
renal_pre AS (
    SELECT DISTINCT o.person_id
    FROM PROD_DWH.ANALYST_PRIMARY_CARE.OBSERVATION o
    JOIN PROD_DWH.ANALYST_PRIMARY_CARE.CONCEPT c ON o.core_concept_id = c.dbid
    JOIN INTELLIGENCE_DEV.AI_CENTRE_DEFINITION_LIBRARY.DEFINITIONSTORE d ON c.code = d.code
    JOIN blinded_dates b ON b.person_id = o.person_id
    WHERE d.definition_name = 'renal_disorder_due_to_DM'
      AND o.clinical_effective_date < b.start_of_blinded_period
),
renal_post AS (
    SELECT DISTINCT o.person_id
    FROM PROD_DWH.ANALYST_PRIMARY_CARE.OBSERVATION o
    JOIN PROD_DWH.ANALYST_PRIMARY_CARE.CONCEPT c ON o.core_concept_id = c.dbid
    JOIN INTELLIGENCE_DEV.AI_CENTRE_DEFINITION_LIBRARY.DEFINITIONSTORE d ON c.code = d.code
    JOIN blinded_dates b ON b.person_id = o.person_id
    WHERE d.definition_name = 'renal_disorder_due_to_DM'
      AND o.clinical_effective_date >= b.start_of_blinded_period
      AND o.clinical_effective_date < b.outcome_date
),
pvd_pre AS (
    SELECT DISTINCT o.person_id
    FROM PROD_DWH.ANALYST_PRIMARY_CARE.OBSERVATION o
    JOIN PROD_DWH.ANALYST_PRIMARY_CARE.CONCEPT c ON o.core_concept_id = c.dbid
    JOIN INTELLIGENCE_DEV.AI_CENTRE_DEFINITION_LIBRARY.DEFINITIONSTORE d ON c.code = d.code
    JOIN blinded_dates b ON b.person_id = o.person_id
    WHERE d.definition_name = 'PVD_or_circulatory_complications'
      AND o.clinical_effective_date < b.start_of_blinded_period
),
pvd_post AS (
    SELECT DISTINCT o.person_id
    FROM PROD_DWH.ANALYST_PRIMARY_CARE.OBSERVATION o
    JOIN PROD_DWH.ANALYST_PRIMARY_CARE.CONCEPT c ON o.core_concept_id = c.dbid
    JOIN INTELLIGENCE_DEV.AI_CENTRE_DEFINITION_LIBRARY.DEFINITIONSTORE d ON c.code = d.code
    JOIN blinded_dates b ON b.person_id = o.person_id
    WHERE d.definition_name = 'PVD_or_circulatory_complications'
      AND o.clinical_effective_date >= b.start_of_blinded_period
      AND o.clinical_effective_date < b.outcome_date
),
mi_pre AS (
    SELECT DISTINCT o.person_id
    FROM PROD_DWH.ANALYST_PRIMARY_CARE.OBSERVATION o
    JOIN PROD_DWH.ANALYST_PRIMARY_CARE.CONCEPT c ON o.core_concept_id = c.dbid
    JOIN INTELLIGENCE_DEV.AI_CENTRE_DEFINITION_LIBRARY.DEFINITIONSTORE d ON c.code = d.code
    JOIN blinded_dates b ON b.person_id = o.person_id
    WHERE d.definition_name = 'myocardial_infarction'
      AND o.clinical_effective_date < b.start_of_blinded_period
),
mi_post AS (
    SELECT DISTINCT o.person_id
    FROM PROD_DWH.ANALYST_PRIMARY_CARE.OBSERVATION o
    JOIN PROD_DWH.ANALYST_PRIMARY_CARE.CONCEPT c ON o.core_concept_id = c.dbid
    JOIN INTELLIGENCE_DEV.AI_CENTRE_DEFINITION_LIBRARY.DEFINITIONSTORE d ON c.code = d.code
    JOIN blinded_dates b ON b.person_id = o.person_id
    WHERE d.definition_name = 'myocardial_infarction'
      AND o.clinical_effective_date >= b.start_of_blinded_period
      AND o.clinical_effective_date < b.outcome_date
),
stroke_pre AS (
    SELECT DISTINCT o.person_id
    FROM PROD_DWH.ANALYST_PRIMARY_CARE.OBSERVATION o
    JOIN PROD_DWH.ANALYST_PRIMARY_CARE.CONCEPT c ON o.core_concept_id = c.dbid
    JOIN INTELLIGENCE_DEV.AI_CENTRE_DEFINITION_LIBRARY.DEFINITIONSTORE d ON c.code = d.code
    JOIN blinded_dates b ON b.person_id = o.person_id
    WHERE d.definition_name = 'non_haemorrhagic_stroke'
      AND o.clinical_effective_date < b.start_of_blinded_period
),
stroke_post AS (
    SELECT DISTINCT o.person_id
    FROM PROD_DWH.ANALYST_PRIMARY_CARE.OBSERVATION o
    JOIN PROD_DWH.ANALYST_PRIMARY_CARE.CONCEPT c ON o.core_concept_id = c.dbid
    JOIN INTELLIGENCE_DEV.AI_CENTRE_DEFINITION_LIBRARY.DEFINITIONSTORE d ON c.code = d.code
    JOIN blinded_dates b ON b.person_id = o.person_id
    WHERE d.definition_name = 'non_haemorrhagic_stroke'
      AND o.clinical_effective_date >= b.start_of_blinded_period
      AND o.clinical_effective_date < b.outcome_date
),
dka_pre AS (
    SELECT DISTINCT o.person_id
    FROM PROD_DWH.ANALYST_PRIMARY_CARE.OBSERVATION o
    JOIN PROD_DWH.ANALYST_PRIMARY_CARE.CONCEPT c ON o.core_concept_id = c.dbid
    JOIN INTELLIGENCE_DEV.AI_CENTRE_DEFINITION_LIBRARY.DEFINITIONSTORE d ON c.code = d.code
    JOIN blinded_dates b ON b.person_id = o.person_id
    WHERE d.definition_name = 'ketoacidosis_due_to_T2DM'
      AND o.clinical_effective_date < b.start_of_blinded_period
),
dka_post AS (
    SELECT DISTINCT o.person_id
    FROM PROD_DWH.ANALYST_PRIMARY_CARE.OBSERVATION o
    JOIN PROD_DWH.ANALYST_PRIMARY_CARE.CONCEPT c ON o.core_concept_id = c.dbid
    JOIN INTELLIGENCE_DEV.AI_CENTRE_DEFINITION_LIBRARY.DEFINITIONSTORE d ON c.code = d.code
    JOIN blinded_dates b ON b.person_id = o.person_id
    WHERE d.definition_name = 'ketoacidosis_due_to_T2DM'
      AND o.clinical_effective_date >= b.start_of_blinded_period
      AND o.clinical_effective_date < b.outcome_date
)

SELECT
    b.person_id,
    CASE WHEN d.person_id IS NOT NULL THEN 1 ELSE 0 END AS diabetic_eye_complications_in_obs_period,
    CASE WHEN e.person_id IS NOT NULL THEN 1 ELSE 0 END AS diabetic_eye_complications_in_outcome_period,
    CASE WHEN f.person_id IS NOT NULL THEN 1 ELSE 0 END AS hf_in_obs_period,
    CASE WHEN g.person_id IS NOT NULL THEN 1 ELSE 0 END AS hf_in_outcome_period,
    CASE WHEN neuropathy_pre.person_id IS NOT NULL THEN 1 ELSE 0 END AS neuropathy_in_obs_period,
    CASE WHEN neuropathy_post.person_id IS NOT NULL THEN 1 ELSE 0 END AS neuropathy_in_outcome_period,
    CASE WHEN amputation_pre.person_id IS NOT NULL THEN 1 ELSE 0 END AS amputation_in_obs_period,
    CASE WHEN amputation_post.person_id IS NOT NULL THEN 1 ELSE 0 END AS amputation_in_outcome_period,
    CASE WHEN renal_pre.person_id IS NOT NULL THEN 1 ELSE 0 END AS renal_in_obs_period,
    CASE WHEN renal_post.person_id IS NOT NULL THEN 1 ELSE 0 END AS renal_in_outcome_period,
    CASE WHEN pvd_pre.person_id IS NOT NULL THEN 1 ELSE 0 END AS pvd_in_obs_period,
    CASE WHEN pvd_post.person_id IS NOT NULL THEN 1 ELSE 0 END AS pvd_in_outcome_period,
    CASE WHEN mi_pre.person_id IS NOT NULL THEN 1 ELSE 0 END AS mi_in_obs_period,
    CASE WHEN mi_post.person_id IS NOT NULL THEN 1 ELSE 0 END AS mi_in_outcome_period,
    CASE WHEN stroke_pre.person_id IS NOT NULL THEN 1 ELSE 0 END AS stroke_in_obs_period,
    CASE WHEN stroke_post.person_id IS NOT NULL THEN 1 ELSE 0 END AS stroke_in_outcome_period,
    CASE WHEN dka_pre.person_id IS NOT NULL THEN 1 ELSE 0 END AS dka_in_obs_period,
    CASE WHEN dka_post.person_id IS NOT NULL THEN 1 ELSE 0 END AS dka_in_outcome_period
FROM blinded_dates b
LEFT JOIN diabetic_eye_complications_pre d ON b.person_id = d.person_id
LEFT JOIN diabetic_eye_complications_post e ON b.person_id = e.person_id
LEFT JOIN hf_pre f ON b.person_id = f.person_id
LEFT JOIN hf_post g ON b.person_id = g.person_id
LEFT JOIN neuropathy_pre ON b.person_id = neuropathy_pre.person_id
LEFT JOIN neuropathy_post ON b.person_id = neuropathy_post.person_id
LEFT JOIN amputation_pre ON b.person_id = amputation_pre.person_id
LEFT JOIN amputation_post ON b.person_id = amputation_post.person_id
LEFT JOIN renal_pre ON b.person_id = renal_pre.person_id
LEFT JOIN renal_post ON b.person_id = renal_post.person_id
LEFT JOIN pvd_pre ON b.person_id = pvd_pre.person_id
LEFT JOIN pvd_post ON b.person_id = pvd_post.person_id
LEFT JOIN mi_pre ON b.person_id = mi_pre.person_id
LEFT JOIN mi_post ON b.person_id = mi_post.person_id
LEFT JOIN stroke_pre ON b.person_id = stroke_pre.person_id
LEFT JOIN stroke_post ON b.person_id = stroke_post.person_id
LEFT JOIN dka_pre ON b.person_id = dka_pre.person_id
LEFT JOIN dka_post ON b.person_id = dka_post.person_id
ORDER BY b.person_id;