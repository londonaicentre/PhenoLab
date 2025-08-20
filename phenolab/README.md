# PhenoLab

Clinical definition management and phenotyping application deployed on Snowflake. Provides a Streamlit interface for managing medical code definitions, standardising measurements, and building features from healthcare data.

This app is written to be integrated with the AI Centre/OneLondon Snowflake dbt data pipeline. However, it can be deployed in isolation given correct Snowflake configuration.

Deployment instructions (and dependencies) for superusers are found in DEPLOYMENT.md.

## Application Pages

### 1. Browse Definitions
Browse and compare clinical definitions across multiple sources including:
- **AIC_DEFINITIONS**: AI Centre custom definitions
- **ICB_DEFINITIONS**: ICB-specific definitions
- **External sources**: HDR UK, OpenCodelists, NHS SNOMED refsets
- Side-by-side comparison tools for validation and refinement

### 2. Manage Definitions
Create, edit, and manage clinical code definitions:
- Build new definitions with vocabulary code browser (SNOMED, ICD10, OPCS4, etc.)
- Modify existing definitions from local JSON files
- Push definitions to Snowflake (local development only)
- Automatic measurement prefix handling and validation

### 3. Standardise Measurements
Configure measurement standardisation and unit conversions:
- Define standard units and mapping from source units
- Specify conversion formulas and acceptable value ranges
- Visual analysis of measurement distributions and outliers
- Set cut-offs which will flag inappropriately high and low values
- Export configurations for measurement processing

## Local vs Server Runtime

### Local Development
When running `streamlit run PhenoLab.py` locally:
- **Default environment**: `dev` (uses dev tables on Snowflake)
- **Production environment (NOT RECOMMENDED)**: Set `DEPLOY_ENV=prod streamlit run PhenoLab.py`
- Optimised for super user creation of new definitions and measurement cleaning
- Connects to Snowflake backend using `~/.snowflake/connections.toml` credentials
- AI Centre definition creation, editing and upload capability
- Full measurement configuration and upload capability
- Update dev feature tables for conditions and measurements based on latest data and definitions

### Server Deployment
When deployed to Snowflake via `deploy.sh`:
- Runs within Snowflake's managed environment
- Optimised for end-user browsing and analysis
- Uses server-side configuration and credentials
- No AI Centre definition or measurement config upload functionality (these are loaded during deployment)
- ICBs can create custom definitions, and these will be reflected in the definition store
- Update dev feature tables for conditions and measurements based on latest data and definitions


