# Setting up a DEFINITIONSTORE in Snowflake

## Clarification of Terms

**Code**, or **Concept**: A single clinical code that can be mapped to a clinical meaning. For example, a single SNOMED code is a `Concept`. This atomic definition aligns with how terminologies are represented in OMOP.

**Codelist**: A collection of multiple `Codes` from the same vocabulary that represent a same clinical meaning. For example, a `Codelist` may contain multiple SNOMED codes that all indicate presence of Diabetes Mellitus. Generally `Codelists` are used to indicate the clinical meaning where there is presence of any (i.e. at least one) of the contained `Codes` in a patient's record.

**Definition**: These are functional definitions with clinical meaning that may contain a single `Codelist` or multiple. For example, a Hypertension `Definition` may include `Codelists` for SNOMED-CT and ICD-10..

## To Use

For entry points to the project, see `update.py` (script to update the existing phenotype table) and `explore.py` (streamlit app for browsing the `Definitions`.) See `loaders/` for load scripts.