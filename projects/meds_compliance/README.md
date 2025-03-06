# Exploring medications compliance
This folder is used to explore the medications tables on snowflake NEL primary care data.

run 'streamlit run lipid_meds.py' for streamlit app prototype.

Please install the requirements and set up an env (see env.example).

## Overal aims:
1. Explore if medications compliance can be summarised/used as a feature i.e Proportion of Days Covered (PDC)
2. Explore if medication complaince alerts/flags can be added to dashboard
3. Explore if undertreated group i.e. not prescribed when eligbile can be defined and flagged.

## Current file status
|File|Status|Task|Task owner|
|-----|-----|-----|------|
|lipid_meds.py|retired|built new app, see below| Jasjot|
|lrd_compliance.py|work in progress|continue work on PDC and treatment switching| Jasjot|
|pdc_analysis.py|work in progress|analysing PDC data| Jasjot|
|
  