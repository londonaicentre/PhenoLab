# PhenoHub design outline

## Page 1 - Concept List Creator

"Concept list" drop down & "Load concept list" button:
1. Lists all parquet files in /concepts
2. Allows one to be selected and loaded to session state as 'concepts'
3. Displays top 100 of concept list ordered by COUNT

"Regenerate concept list" button:
1. Executes hard-coded SQL queries that pull a set of distinct concepts/counts from multiple Snowflake tables, and unions them into a table of unique concepts. 2. Saves result as a Parquet file in /concepts named by datetime
3. Displays top 100 of concept list ordered by COUNT

Concept list table has the columns: CONCEPT_CODE; CONCEPT_NAME; CONCEPT_COUNT; VOCABULARY; CONCEPT_TYPE

## Page 2 - Definitions Creator

This page has three panels (one in first row, two in second row)

### What is a definition?

* Definition = a collection of Codelist(s) that define a simple concept
* Codelist = a set of Codes in the same vocabulary
* Code = a single Code representing a discrete clinical concept

E.g. a Definition of Hypertension could contains a READV2 Codelist and a SNOMED Codelist, each containing a list of codes

Custom definitions are stored in .json in /definitions. This structure should have the following fields:
* "definition_id": 8 digit hash of name and version
* "definition_name": User entered name
* "definition_version": {name}_{lateupdateddatetime}
* "definition_source": hardcoded to be "CUSTOM"
* "version_datetime": datetime when created,
* "updated_datetime": datetime when last updated,
* "definiton_type": e.g. medication / observation / disorder

REPEATABLE BLOCK:
* "codelist_id": 8 digit hash of name and version,
* "codelist_name": {definitionname}_{vocabulary}
* "codelist_version": {codelistname}_{lastupdateddatetime}

REPEATABLE BLOCK INSIDE CODELIST BLOCK
* "code": CONCEPT_CODE,
* "code_description": CONCEPT_NAME,
* "vocabulary": VOCABULARY

### Panel 1 (first row)

"Custom definition list" drop down & "Edit definition list" button:
1. Lists all json files in /definitions
2. Allows one to be selected and loaded to session state as 'definitions'
3. Displays all codes in the definition in Panel 3 (see below)

"New definition name" box & "Create new definition" botton:
1. Enters a name for a new definition that is saved to a state
2. Once Create button pressed, blank space appears in Panel 3 (see below) ready to be populated by codes

### Panel 2 (left, second row)

"Filter concepts" search box, with "Search" button. Concept type drop down that filters based on CONCEPT_TYPE.
1. Type in keywords + search, select a concept type to filter.
2. Filtered concepts appear in panel and are individually selectable
3. Shows concept name, concept code, concept count, concept vocabulary
4. As concepts as selected, they appear on Panel 3 (see below)

Possible add all selected concepts to an array in memory, that is read to see what remains selected, and is also read to populate Panel 3. 

### Panel 3 (right, second row)

View of all concepts that are individually selected in Panel 1. Shows concept name and concept code, ordered by concept name descending.

"Save definition" button
1. If it is a new defintiion, With name that was previously entered, the save button will create a new json file
2. If it is an existing definition or previously saved, the button will update the existing json file.
2. Within the definition, codelists are created as groupings of unique vocabularies (e.g. all SNOMED goes to one codelist, all ICD10 goes to another codelist)



