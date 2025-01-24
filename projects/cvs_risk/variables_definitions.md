# Study design and variable definitions table for logistic regression CVS risk
Simple study design to start with - can build on study design, cohort definitation and variables

To do:
[] find the phenotypes for variables that require (see table)
[] create tables for each phenotype for the defined cohort (i.e make feature table)
[] explore each feature with a dashboard - check missingness, plausiblility of values and unadjusted associations
[] create basic logistic regression : Outcome ~ predictors ? use train test if using for prediction

Study design:
Retrospective cohort study

Cohort:
- Start date/index date: lastest from date turn 18 or date registered
- End date: earliest from date of death, date deregistred or today/most recent administerative date
- At least one year registration for look back period
- No histroy of CVD outcome on index date

Outcome:
- CVD (Stroke, TIA, MI, CHD, AF, HF, PAD?)
- Taken from first hosp admission with primary admission reason

Predictors:
- See table below

Analysis:
- Logistic regression: Outcome ~ predictors
- Adjusted Odds Ratios for each ?binary CVS predictor
- No temporal assocation, just outcome ~ predictor association



| Variable\Feature | Definition  | Phenotypes/variables needed |Variable type | Phenotype req|
|------------|------------|------------|-----------|
| Registration date| Date registered to the GP|Episode of care table (date registered?) - need to explore this table more| Date|
| Date of death | If dead, what date| Patient table (date of death) | Date|
| Deregistration or administrative end date | Administrative censoring date| Unsure - need to check episode of care table?| Date|
| DOB | Date of Birth| Patient table(YOB, Day of Birth, Week of birth)| Date|
| Ethnicity | Ethnic category - check groupings align with cencus coding, if so which? | Patient table (ethnicity) | Categorical |
| IMD | Index of Multiple Deprivation Decile (or quintile) | Patient Address table (LSOA2011) - need to link to IMD| Categorical |
| Stroke | Stroke after index date | Observation table?(core_concept_ID for stroke codes) | Binary (for now)| Y|
|TIA |TIA after index date | Observation table?(core_concept_ID for TIA codes) | Binary (for now)| Y|
| IHD/CHD|IHD after index date | Observation table?(core_concept_ID for IHD codes) | Binary (for now)|  Y|
| CKD| CKD after index date | Observation table?(core_concept_ID for CKD codes) | Binary (for now)|  Y|
| Diabetes |Diabetes after index date | Observation table?(core_concept_ID for Diabetes codes) | Binary (for now)|  Y|
| AF | AF after index date | Observation table?(core_concept_ID for AF codes) | Binary (for now)|  Y|
| PAD |PAD after index date | Observation table?(core_concept_ID for PAD codes) | Binary (for now)|  Y|
| HF |HF after index date | Observation table?(core_concept_ID for HF codes) | Binary (for now)|  Y|
| Vasc Dem | Vasc Dem after index date | Observation table?(core_concept_ID for Vasc dem codes) | Binary (for now)|  Y|
| Statin |Prescribed statin after index date |  Medication Order/statement and BNF codes for statin| Binary (for now)|  Y|
| BMI |  ? average BMI or BMI at index? or overweight binary code | Observation table (core concept_id for BMI, or calculate from height and weight) | ?categorical vs cont| Y|
| Alcohol | ? average weekly units OR heavy drinker binary code | Observation table (core concept_id for acohol units weekly) | ?categorical vs cont vs binary| Y|
| Admissions | number hosp admissions | ? Encounter table - need to explore | cont?|
| FH_CHD | family history of CHD | Observation table (core_concept_id) | binary| Y|
| HTN |HTN after index date | Observation table?(core_concept_ID for HTN codes) | Binary (for now)|  Y|
| Familial Hypercholesterolaemia | f_hyp after index date | Observation table?(core_concept_ID for f_hyp codes) | Binary (for now)|  Y|
| Hypercholesterolaemia |  hyp after index date | Observation table?(core_concept_ID for hyp codes) | Binary (for now)|  Y|
| QRISK | ?qrisk at baseline | Observation table?(core_concept_ID for qrisk codes) | Binary (for now)|  Y|
| Non-CVS comorbidities | need to think about more| Y|
| number of comorbidities | number of total comorbs at baseline | observation table | cont| Y|
| Smoker| current smoker at baseline/index | observation table | binary| Y|
