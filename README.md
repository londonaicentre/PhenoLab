# onelondon_snowflake_datascience
Data Science repository containing utility functions and interactive tooling for population health analytics and machine
learning in OneLondon Snowflake

## Project Structure

- `src/phmlondon` - Installable utilities for reusable functions for doing pop health data snowflake on Snowflake
  environments
- `/phenolab` - Code for phenolab, an app for creating and import codelists for defining population segments

## Set up a snowflake connection
Create a `.env` file which includes the connection variables:

~~~
## SNOWFLAKE CONNECTION VARIABLES
SNOWFLAKE_SERVER=snowflake_server
SNOWFLAKE_USER=snowflake_user
SNOWFLAKE_USERGROUP=snowflake_usergroup
~~~

Make sure you have python-dotenv installed

Set up your python code something like this. Snowflake will open a browser window to authenticate the connection.

~~~python
from dotenv import load_dotenv
from phmlondon.snow_utils import SnowflakeConnection
import logging # optional, for debugging

logging.basicConfig(level=logging.DEBUG) # optional, for debugging

load_dotenv()
conn = SnowflakeConnection()
conn.use_database("INTELLIGENCE_DEV")
conn.use_schema("AI_CENTRE_DEV") # adjust as appropriate
    
conn.list_tables() # example of command you could run once connected to the database.
~~~~

## Set up a snowflake connection for PhenoLab

Phenolab uses Snowpark utilities to connect to Snowflake, rather than PHMLondon utils, so the above method using .env
variables won't work.

Either use [this page](https://docs.snowflake.com/en/developer-guide/snowpark/python/creating-session) to set up an
connections.toml file (an example is given in the /Phenolab directory), or set up a default connnection on the Snowflake
CLI - 
[this internal tutorial](https://github.com/londonaicentre/sde_aic_internal_docs/blob/main/infra/snowflake_cli_setup.md)
may help.

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