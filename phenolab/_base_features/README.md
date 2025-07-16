# Base Features

This directory contains scripts for creating base feature tables. These scripts should be executed **before** running
the main PhenoLab deployment (`deploy.sh`) to ensure the base feature tables are available for use in phenotype
definitions and feature engineering.

### `base_apc_concepts.py`
Creates the `BASE_APC_CONCEPTS` table containing all primary and secondary diagnoses, procedures, and investigations
from inpatient hospital activity (Admitted Patient Care).

### `base_person_nel_index.py`
Creates the `BASE_PERSON_NEL_INDEX` table containing sociodemographic and spatioeconomic data for each unique person
registered in North East London.
- Requires `IMD2019LONDON` table in `INTELLIGENCE_DEV.AI_CENTRE_EXTERNAL` schema

### `base_unified_emergency_care.py`
Creates the `BASE_UNIFIED_EMERGENCY_CARE` table containing all diagnoses from emergency department encounters.

### `base_unified_sus_encounters.py`
Creates the `BASE_UNIFIED_SUS_ENCOUNTERS` table containing administrative information about all hospital encounters.

### `v_medication_with_bnf.py`
Creates the `V_MEDICATION_WITH_BNF` view that allows browsing and querying by named BNF class.