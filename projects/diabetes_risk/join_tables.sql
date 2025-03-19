create table if not exists intelligence_dev.ai_centre_feature_store.t2dm_patients_with_hba1c_bins as
SELECT 
    p.dob,
    p.dod,
    p.dod_inc_codes,
    p.approx_current_age,
    p.gender_concept_id,
    p.ethnic_code_concept_id,
    h.*
FROM INTELLIGENCE_DEV.AI_CENTRE_FEATURE_STORE.T2DM_PATIENTS_CODED_AS as p
JOIN INTELLIGENCE_DEV.AI_CENTRE_FEATURE_STORE.T2DM_HBA1C_BINNED as h
ON p.person_id = h.person_id;