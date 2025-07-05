# PhenoLab

Clinical definition management and phenotyping application deployed on Snowflake.

## Deployment Stages

### 1. External Definitions Fetching
Run these separately to fetch external definition data and save to parquet files.

```bash
cd _external_definitions
python fetch_hdruk.py
python fetch_open_codelists.py
python fetch_bnf.py
```

### 2. Base Feature Tables
These are essential tables on which subsequent features are built. Currently for NEL ICB only.

```bash
cd _base_features
python base_apc_concepts.py
python base_person_nel_index.py
python base_unified_emergency_care.py
python base_unified_sus_encounters.py
```

### 3. Deployment Process
Deploy Streamlit app and load all data to Snowflake.

```bash
./deploy.sh <ICB> <environment>
# Example: ./deploy.sh nel prod
```

This script:
- Deploys Streamlit app to Snowflake
- Runs setup.py which performs these steps:
  1. Creates ICB_DEFINITIONS table (if not exists)
  2. Loads AIC definitions from JSON files (overwrites where new version)
  3. Loads external definitions from parquet files (overwrites whole table)
  4. Ensures DEFINITIONSTORE unified view
  5. Loads measurement configurations (overwrites)
  6. Creates BASE_APC_CONCEPTS feature table

### 4. Running Application
Access the deployed Streamlit app in Snowflake or run locally:

```bash
streamlit run PhenoLab.py
```

## Connections

### Snowflake CLI Configuration
Configure connections in `~/.snowflake/connections.toml`:

```toml
[nel_icb]
account = "your-account"
user = "your-username"
authenticator = "externalbrowser"
```

### Streamlit Secrets (Local Development)
Configure `.streamlit/secrets.toml` for local Streamlit:

```toml
[connections.snowflake]
account = "your-account"
user = "your-username"
authenticator = "externalbrowser"
role = "your-role"
warehouse = "your-warehouse"
database = "your-database"
schema = "your-schema"
```

### Environment Configuration
ICB-specific configurations in `configs/`:
- `nel_icb_prod.yml`
- `nel_icb_dev.yml`
