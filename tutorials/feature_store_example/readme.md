# Snowflake feature store manager

This folder contains code showing how to use the FeatureStoreManager class, which lives in the phmlondon base package 
and assists with managing metadata tables for machine learning features created in SQL.

To create a feature store, make sure the database and schema where the feature store will live exist on Snowflake. 
Optionally, you can have the metadata tables live in a separate schema to the feature tables, in which case you need to 
pass in a second schema, called `METADATASCHEMA`. Then run the code below to create the metadata tables for the feature 
store. Example code in `create_feature_store.py`. Note that you are unlikely to need to run code like this, as a 
shared feature store already exists on Snowflake:

```python
from dotenv import load_dotenv
from phmlondon.feature_store_manager import FeatureStoreManager
from phmlondon.snow_utils import SnowflakeConnection

DATABASE = "INTELLIGENCE_DEV"
SCHEMA = "AI_CENTRE_FEATURE_STORE"
METADATASCHEMA = "AI_CENTRE_FEATURE_STORE_METADATA"
load_dotenv()
conn = SnowflakeConnection()
feature_store_manager = FeatureStoreManager(conn, DATABASE, SCHEMA, METADATASCHEMA)
feature_store_manager.create_feature_store()
```
Three metadata tables are created. 
- **Feature registry**: For each feature: contains feature unique ID, name, description, format, amd the date of registration
- **Feature version registry**: For each version of a feature: contains feature ID, feature version, name of the table
containing the feature, the SQL query used to create the table, a description of the change made by this version and the
date of version registration. When a new feature is created, the first version is automatically registered here.
- **Active features**: Not used currently, but for registering features used by live models

Since we already have a feature store set up and populated with the relevant tables, you won't need to do the step above
most of the time. Initialise the feature store manager class with the details of the database and schema, and then use 
it to create a feature like this (see `create_feature_hypertension.py`):

```python
from dotenv import load_dotenv
from phmlondon.feature_store_manager import FeatureStoreManager

from phmlondon.snow_utils import SnowflakeConnection

load_dotenv()
conn = SnowflakeConnection()
DATABASE = "INTELLIGENCE_DEV"
SCHEMA = "AI_CENTRE_FEATURE_STORE"
METADATASCHEMA = "AI_CENTRE_FEATURE_STORE_METADATA"
feature_store_manager = FeatureStoreManager(conn, DATABASE, SCHEMA, METADATASCHEMA)

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
Note the the add_new_feature function has an `existence_ok` argument which defaults to False but can be set to True to 
prevent an error being thrown if the feature already exists.

To update a feature (add a new version):

```python
fid = feature_store_manager.get_feature_id_from_table_name('hypertension_v1')
new_version_id = feature_store_manager.update_feature(
    feature_id=fid,
    new_sql_select_query=query,
    change_description="A test update; same query"
)
```

The update feature has two optional flags. If `overwrite=True` is used, the update to the feature will overwrite the
existing registry entry and table. Use this when you are incrementing rapidly in feature design during model development
and don't want a full record of each SQL query used and the resulting table. Defaults to False. If 
`force_new_version=True`, the default behaviour *not* to create a new version if the SQL is idential is overrode. The 
intention of the default behaviour is to prevent a new version being created if the code to update a feature is
accidentally rerun.

The `refresh_latest_feature_version` will 'refresh' the latest version of an existing feature by dropping the table and 
recreating it with the same SQL query used to create it previously. This is good when the query hasn't changed but the
underlying data has.

```python
featureid = feature_store_manager.get_feature_id_from_table_name('HBA1C_FEATURES_V1')
feature_store_manager.refresh_latest_feature_version(featureid)
```

This function will remove a feature version (registry entry and corresponding table). It is only designed for use during
development:

```python
featureid = feature_store_manager.get_feature_id_from_table_name('PATIENTS_WITH_DIABETES_ALL_V1')
feature_store_manager.remove_latest_feature_version(featureid)
```

There is also a function to delete features that are created in error - use with caution and not to retire features that 
have been used in models. It will delete all the feature's tables and registry entries.

```python
fid = feature_store_manager.get_feature_id_from_table_name('hypertension_v1')
feature_store_manager.delete_feature(fid)
```

# Use on NEL Snowflake

- Main feature store:
    - SCHEMA = "AI_CENTRE_FEATURE_STORE"
    - METADATASCHEMA = "AI_CENTRE_FEATURE_STORE_METADATA"
- Test feature store:
    - SCHEMA: "TEST_FEATURE_STORE_IW_2"
    - Don't pass in a metadataschema, and the metadata tables will live in the same schema

Example use of test feature store:
```python
DATABASE = "INTELLIGENCE_DEV"
SCHEMA = "TEST_FEATURE_STORE_IW_2"
load_dotenv()
conn = SnowflakeConnection()
# #we don't pass in a metadata schema, so it will default to using the same schema as for the feature tables
feature_store_manager = FeatureStoreManager(conn, DATABASE, SCHEMA)
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