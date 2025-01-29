# Setting up a phenotype library in Snowflake

## Clarification of Terms

**Code**, or **Concept**: A single clinical code that can be mapped to a clinical meaning. For example, a single SNOMED code is a `Concept`. This atomic definition aligns with how terminologies are represented in OMOP.

**Codelist**: A collection of multiple `Codes` from the same vocabulary that represent a same clinical meaning. For example, a `Codelist` may contain multiple SNOMED codes that all indicate presence of Diabetes Mellitus. Generally `Codelists` are used to indicate the clinical meaning where there is presence of any (i.e. at least one) of the contained `Codes` in a patient's record.

**Phenotype**: These are functional definitions of clinical meaning that can be derived through inclusion of concept codes +/- clinical logic. A `Phenotype` could contain a single `Codelist` or multiple. It could also contain a `Codelist` and a set of rules. For example, a Hypertension `Phenotype` may include `Codelists` for SNOMED-CT, ICD-10, inclusion based on BNF medications, and sequential blood pressure measurements discovered on SNOMED-CT codes.

## Code 

For entry points to the project, see `update.py` (script to update the existing phenotype table) and `visualise.py` (streamlit app for browsing the phenotypes.) See also the `load_` scripts.