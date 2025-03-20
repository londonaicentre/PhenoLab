# Snowflake feature store manager

This folder contains code for a FeatureStoreManager class, which assists with managing metadata tables for machine
learning features created in SQL.

To create a feature store, make sure the database and schema where the feature store will live exist on Snowflake. Then
run the code below to create the metadata tables for the feature store. 
Example code in `create_feature_store.py`:

```python
from dotenv import load_dotenv
from feature_store_manager import FeatureStoreManager

from phmlondon.snow_utils import SnowflakeConnection

DATABASE = "INTELLIGENCE_DEV"
SCHEMA = "AI_CENTRE_FEATURE_STORE"
load_dotenv()
conn = SnowflakeConnection()
feature_store_manager = FeatureStoreManager(conn, DATABASE, SCHEMA)
feature_store_manager.create_feature_store()
```
Three metadata tables are created. 
- **Feature registry**: For each feature: contains feature unique ID, name, description, format, amd the date of registration
- **Feature version registry**: For each version of a feature: contains feature ID, feature version, name of the table
containing the feature, the SQL query used to create the table, a description of the change made by this version and the
date of version registration. When a new feature is created, the first version is automatically registered here.
- **Active features**: Not used currently, but for registering features used by live models

To create a feature, do something like this (see `create_feature_hypertension.py`):

```python
from dotenv import load_dotenv
from feature_store_manager import FeatureStoreManager

from phmlondon.snow_utils import SnowflakeConnection

load_dotenv()
conn = SnowflakeConnection()
DATABASE = "INTELLIGENCE_DEV"
SCHEMA = "AI_CENTRE_FEATURE_STORE"
feature_store_manager = FeatureStoreManager(conn, DATABASE, SCHEMA)

with open("htn_query.sql", "r") as fid:
    query = fid.read()

feature_store_manager.add_new_feature(
    feature_name="Hypertension",
    feature_desc=""""
        Hypertension either diagnosed by codelist, by ambulatory readings or by 3 clinic readings. Hypertension is 
        categorised as stage 1 if the patient has an ambulatory reading between 135/85 and 149/94 or 3 clinic readings 
        from 140/90 to 179/119 in 1 year, and as stage 2 if the patient has an ambulatory reading above 150/95 or 
        3 clinic readings above 180/120 in 1 year. Patients diagnosed by codelist do not have a stage 1/2 flag.
        """,
    feature_format="Binary/categorical",
    sql_select_query_to_generate_feature=query)
```
The feature table will be created and the feature will be registred in the feature registry and feature version registry.

To update a feature (add a new version):

```python
fid = feature_store_manager.get_feature_id_from_table_name('hypertension_v1')
new_version_id = feature_store_manager.update_feature(
    feature_id=fid,
    new_sql_select_query=query,
    change_description="A test update; same query"
)
```

# Project to explore feature store implementations

As well as the code described above, to create a feature store composed of static tables, this folder contains older
versions of code which were used to explore possibilities around creating a live updating feature store.

## Options 
(1) Manual implementation: use `dynamic_table_implementation/feature-store.py` for a class which gives the functionality
to set up and manage a feature store which feature tables and metadata tables

(2) Snowflake implementation using snowflake's feature store capabilities - see `snowflake_fs_implementation/snowflake_feature_store.ipynb`

## Notes
These implementations are not currently used due to snowflake limitations, but the code is retained here for future use.
The issues were to do with dynamic tables in snowflake and lack of access to tasks/dbt:
- Dynamic tables in Snowflake can not incremental refresh with non-deterministic functions e.g. current timestamp. Including
a timestamp would have forced a full refresh of the table each time and an overwrite of the timestamp.
- Dynamic tables in Snowflake can be used on underlying views, but this strongly restricts the complexity of the queries 
possible e.g. no group by, CTEs.