## Project Structure

- `src/phmlondon` - Installable utilities for reusable functions for doing pop health data snowflake on Snowflake
  environments
- `/phenolab` - Code for phenolab, an app for creating and import codelists for defining population segments

## Deploy streamlit using bash script

Navigate to the phenolab folder and set up the deploy.sh script as executable (`chmod +x deploy.sh`).

Use the script like this:
```
bash deploy.sh <icb name> <dev/prod>
```

To deploy to the dev version on NEL ICB's snowflake:
```
bash deploy.sh nel dev
```
This version of the app is called PHENOLAB_DEV and has its own schema with dummy tables. Use for testing ICB deployment.


To deploy to the prod version:
```
bash deploy.sh nel prod
```
This deploys to PHENOLAB and the live tables. Use for pushing changes to the ICB deployment to live.

These two apps can be acessed via the Snowflake interface, Snowsight.


To use the app on localhost for internal AI centre use, run:
```
streamlit run PhenoLab.py
```
This defaults to the production environment i.e. uses the live tables. Use for generating new definitions and adding to 
our json store.


To use the app on localhost but use the dev tables (use for testing out new Phenolab code), set a temporary
environmental variable when running streamlit:
```
DEPLOY_ENV=dev streamlit run PhenoLab.py
```