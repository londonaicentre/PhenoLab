# Medication compliance survival analysis
## Study type
Time varying survival analysis

Need to think about
- Do we want to look at impact on direct measures such as blood results etc.
- Or do we want to look at impact on indirect outcomes like death/CVD outcomes.

### Study period
2004-2024

### Cohort
1. Individuals with CVD risk factors
2. On one or more Long Term meds
3. Index date - date of first Long term medication
4. Exposure - PDC Time varying by month and drug

### Outcomes
1. Death
2. CVD Death
3. CVD Outcome - stroke, MI, TIA, HF??

### Censoring
1. Outcome
2. Loss to follow up
3. No more Long Term medicaitons

### covariates (some can be time varying)
1. IMD
2. Ethnicity
3. Smoker
4. BMI
5. comorbidities
6. number of medications
7. bloods?


## To Do
[ ] Make or find definitions for each variable

[ ] Write SQL for cohort selection

[ ] Write SQL for each variable

[ ] clean each variable table/check missingness/plausibility/distrubutions