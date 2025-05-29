# onelondon_snowflake_datascience
Data Science repository containing utility functions and interactive tooling for population health analytics and machine learning in OneLondon Snowflake

## Project Structure

- `src/phmlondon` - Installable utilities for reusable functions for doing pop health data snowflake on Snowflake environments
- `pipeline/phenolab` - Code for phenolab, an app for creating and import codelists for defining population segments

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
