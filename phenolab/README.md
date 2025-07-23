# PhenoLab

Clinical definition management and phenotyping application deployed on Snowflake.

## Deployment Stages

### 1. External Definitions Fetching
Run these separately to fetch external definition data and save to parquet files.

```bash
cd phenolab
python _external_definitions/fetch_hdruk.py
python _external_definitions/fetch_open_codelists.py
python _external_definitions/fetch_bnf.py
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
Deploy Streamlit app and load all data to Snowflake. Navigate to the phenolab folder and set up the deploy.sh script as
executable (`chmod +x deploy.sh`). Then run the deployment script, with an ICB name and either dev or prod. Prod is the
live app. Dev is a second version of the app is called PHENOLAB_DEV and has its own schema with dummy tables. Use for
testing ICB deployment:

```bash
./deploy.sh <ICB> <environment>
# Example prod: ./deploy.sh nel prod
# Example dev: ./deploy.sh nel dev
```

The deployment script:
- Deploys Streamlit app to Snowflake
- Runs setup.py which performs these steps:
  1. Creates ICB_DEFINITIONS table (if not exists)
  2. Loads AIC definitions from JSON files (overwrites where new version)
  3. Loads external definitions from parquet files (merges into existing tables)
  4. Ensures DEFINITIONSTORE unified view
  5. Loads measurement configurations (overwrites)

### 4. Running Application
Access the deployed Streamlit app in Snowflake or, to run locally with backend connection to the Snowflake prod tables.
This defaults to the production environment i.e. uses the live tables. Use for internal AI centre use for generating new
definitions and adding to our json store:

```bash
streamlit run PhenoLab.py
```

To use the app on localhost but use the dev tables (use for testing out new Phenolab code), set a temporary
environmental variable when running streamlit:

```bash 
 `DEPLOY_ENV=dev streamlit run PhenoLab.py` for running local and in dev (uses dev tables on snowflake)
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

### Environment Configuration
ICB-specific configurations in `configs/`:
- `nel_icb_prod.yml`
- `nel_icb_dev.yml`
