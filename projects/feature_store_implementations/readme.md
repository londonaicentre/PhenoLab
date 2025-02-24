# Project to explore feature store implementations

## Options 
(1) Manual implementation: use `feature-store.py` for a class which gives the functionality to set up and manage a feature store which feature tables and metadata tables

(2) Snowflake implementation using snowflake's feature store capabilities - see `snowflake_feature_store.ipynb`

## Notes
- Feature tables will be dynamic tables i.e. will update automatically on a fixed refresh schedule. The other option would be event-driven updates, by having static tables, and then setting up a snowflake stream to detect updates and a snowflake task to merge the updates into the existing tables. However, this is more complex to setup, so would require more prior knowledge on part of user, or otherwise would have to script these commands somehow. (If used views, would have to recompute the query from scratch each time. Storage is much cheaper than compute in snowflake so this doesn't make sense.)