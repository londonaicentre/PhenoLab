/*
Analyst: Jordan

Requires index_date to be set before this query is executed (must be last day of a month).

Takes admission and appt activity for 5 years after the index date.

Joins these to both the PMI, segmentation, and CMM flag table to include all data required for
     training and evaluating the CMM.

Binary death/admission/appt flags work as event censors 
Time until death/admission/appt calculates the time until first event from the index date 

 */

with sus as (
SELECT sk_patientid, 
       count(*) as emergency_admissions_5yr,  
       sum(CASE WHEN apc.discharge_date BETWEEN $index_date AND dateadd(year, 1, $index_date) THEN 1 ELSE 0 END) as emergency_admissions_1yr,
       min(discharge_date) as first_admission
FROM prod_dwh.analyst_facts_unified_sus.unified_sus_admitted_patient_care apc
WHERE apc.discharge_date between $index_date AND dateadd(year, 5, $index_date)
    AND admission_sub_type = 'Emergency'
GROUP BY sk_patientid
)

, gp_appts as (
SELECT p.person_id, 
       count(*) as gp_appts_5yr,
       sum(CASE WHEN contact_date BETWEEN $index_date AND dateadd(year, 1, $index_date) THEN 1 ELSE 0 END) as gp_appts_1yr,
       min(contact_date) as first_appt
FROM prod_dwh.analyst_facts.primary_care_encounters_and_online_consults appts
    LEFT JOIN prod_dwh.analyst_primary_care.patient p
        ON p.id = appts.patient_id
WHERE appts.contact_date BETWEEN $index_date AND dateadd(year, 5, $index_date)
    AND clinician_category = 'General Practitioner'
    AND appts.encounter_flag = 1
GROUP BY p.person_id
)

, psychoactive as (
SELECT person_id, 1 as substance_dependence
FROM prod_dwh.analyst_primary_care.observation o
    LEFT JOIN prod_dwh.analyst_primary_care.concept c
        ON o.core_concept_id = c.dbid
    LEFT JOIN intelligence_dev.phm_segmentation.segmentation_codes codes
        ON codes.code = c.code
WHERE condition_upper = 'SUBSTANCE DEPENDENCE' -- not complete as isn't including SUS for now
    AND clinical_effective_date <= $index_date
)

SELECT  pmi.sk_patientid,
        pmi.age, -- Do we want to censor age at 95 like in the Modified CMM paper? 
        square(pmi.age) as age_squared,
        pmi.imd_quintile,
        pmi.imd_decile,
        pmi.gender, 
        pmi.registration_start_date,
        pmi.practice_code,
        pmi.practice_name,
        pmi.borough_of_registered_gp_practice,
        CASE WHEN gender = 'Male' THEN 1 ELSE 0 END AS sex,
        flags.alcoholproblems,
        fs.healthy_with_risk_factors_alcohol_dependence,
        
        flags.anxietyordepression,
        fs.healthy_with_risk_factors_depression,
        
        flags.atrialfibrillation,
        fs.ltc_atrial_fibrillation,
        
        flags.cancerinthelast5years,
        fs.ltc_cancer,

        flags.chronickidneydisease,
        fs.ltc_chronic_kidney_disease,

        flags.chronicliverdiseaseandviralhepatitis,
        fs.ltc_chronic_liver_disease,

        flags.constipation,
        0 as ltc_constip, --not in segmentation

        flags.copd,
        fs.ltc_chronic_obstructive_pulmonary_disease,

        flags.dementia,
        fs.frailty_and_dementia_dementia,

        flags.diabetes,
        fs.ltc_diabetes,

        flags.disorderofprostate,
        0 as prostate, -- not in segmentation

        flags.epilepsy,
        fs.ltc_epilepsy,

        flags.heartfailure,
        fs.ltc_heart_failure,

        flags.irritablebowelsyndrome,
        fs.ltc_inflammatory_bowel_disease,

        flags.learningdisability,
        fs.disability_learning_disability,

        flags.multiplesclerosis,
        fs.ltc_multiple_sclerosis,

        flags.painfulcondition,
        fs.ltc_chronic_pain,

        flags.parkinsonism,
        fs.ltc_parkinsons_disease,

        flags.periphvascdiseaseleg,
        fs.ltc_peripheral_vascular_disease,

        flags.psychoactivesubstancemisuse,
        psychoactive.substance_dependence,

        flags.schizophreniaorbipolardisorder,
        fs.smi_serious_mental_illness,

        -- Deaths
        pmi.date_of_death_inc_codes,
        CASE WHEN datediff(days, $index_date, date_of_death) < 365 THEN datediff(days, $index_date, date_of_death) ELSE 365 END AS time_to_death_1yr,
        CASE WHEN datediff(days, $index_date, date_of_death) < 365 * 5 THEN datediff(days, $index_date, date_of_death) ELSE 365 * 5 END AS time_to_death_5yr,
        CASE WHEN datediff(days, $index_date, date_of_death) < 365 THEN 1 ELSE 0 END AS death_1yr,
        CASE WHEN datediff(days, $index_date, date_of_death) < 365 * 5 THEN 1 ELSE 0 END AS death_5yr,
        
        -- Admissions
        sus.emergency_admissions_5yr,
        sus.emergency_admissions_1yr,
        sus.first_admission,
        CASE WHEN datediff(days, $index_date, first_admission) < 365 THEN datediff(days, $index_date, first_admission) ELSE 365 END AS time_to_adm_1yr,
        CASE WHEN datediff(days, $index_date, first_admission) < 365 * 5 THEN datediff(days, $index_date, first_admission) ELSE 365 * 5 END AS time_to_adm_5yr,
        CASE WHEN datediff(days, $index_date, first_admission) < 365 THEN 1 ELSE 0 END AS adm_1yr,
        CASE WHEN datediff(days, $index_date, first_admission) < 365 * 5 THEN 1 ELSE 0 END AS adm_5yr,

        -- Appts
        gp_appts.gp_appts_1yr,
        gp_appts.gp_appts_5yr,
        gp_appts.first_appt,
        CASE WHEN datediff(days, $index_date, first_appt) < 365 THEN datediff(days, $index_date, first_appt) ELSE 365 END AS time_to_appt_1yr,
        CASE WHEN datediff(days, $index_date, first_appt) < 365 * 5 THEN datediff(days, $index_date, first_appt) ELSE 365 * 5 END AS time_to_appt_5yr,
        CASE WHEN datediff(days, $index_date, first_appt) < 365 THEN 1 ELSE 0 END AS appt_1yr,
        CASE WHEN datediff(days, $index_date, first_appt) < 365 * 5 THEN 1 ELSE 0 END AS appt_5yr,
        
FROM prod_dwh.analyst_facts.pmi pmi
    LEFT JOIN intelligence_dev.ai_centre_dev.cambridge_comorb_2022_flag_table flags 
        ON pmi.person_id = flags.person_id
        AND pmi.end_of_month = flags.end_of_month
    LEFT JOIN prod_dwh.analyst_facts_segmentation.final_segment fs
        ON pmi.sk_patientid = fs.sk_patientid
        AND pmi.end_of_month = fs.end_of_month
    LEFT JOIN sus
        ON sus.sk_patientid = pmi.sk_patientid
    LEFT JOIN gp_appts
        ON gp_appts.person_id = pmi.person_id

    LEFT JOIN psychoactive
        ON pmi.person_id = psychoactive.person_id
WHERE include_in_list_size_flag = 1
AND pmi.end_of_month = $index_date
AND (date_of_death is null or date_of_death > pmi.end_of_month) -- Exclude people who died same month
