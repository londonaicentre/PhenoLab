# Setting up a DEFINITIONSTORE in Snowflake

## Clarification of Terms

**Code**, or **Concept**: A single clinical code that can be mapped to a clinical meaning. For example, a single SNOMED code is a `Concept`. This atomic definition aligns with how terminologies are represented in OMOP.

**Codelist**: A collection of multiple `Codes` from the same vocabulary that represent a same clinical meaning. For example, a `Codelist` may contain multiple SNOMED codes that all indicate presence of Diabetes Mellitus. Generally `Codelists` are used to indicate the clinical meaning where there is presence of any (i.e. at least one) of the contained `Codes` in a patient's record.

**Definition**: These are functional definitions with clinical meaning that may contain a single `Codelist` or multiple. For example, a Hypertension `Definition` may include `Codelists` for SNOMED-CT and ICD-10..

## To Use

- scripts should be run from the PhenoLab folder
- `loaders/fetch_hdruk.py` and `loaders/fetch_open_codelists.py`: standalone scripts which retrieve definitions from
relevant sources put them into the defintion format and export them as a csv in the /data folder. (Choice of definitions
specified by variables in the script)
- the various loader scripts define functions which can be called to load definitions from the data folder to snowflake. 
`loaders/create_tables.py` has the functions for doing the table merge.
- `/data`: static definition data
- `create_definitionstore_view.py`: creates the 'definitionstore' view on snowflake which will then dynamically pull
from the individual tables. This only needs to be run once at setup and then only rerun if the underlying table
structure changes or new tables are  added.
