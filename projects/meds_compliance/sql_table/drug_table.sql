-- SQL code to create a durg table that can be easily joined with the primary care medications table.
-- the table contains the core concept id which will join to the core-concept id in the medications table
-- the other columns are the ful drug description, the clean drug name, and the drug class.
CREATE OR REPLACE TABLE intelligence_dev.ai_centre_dev.drug_table_v4 AS
SELECT DISTINCT
    m.code_description as drug_description,
    m.definition_name as drug_name,
    h.codelist_name as class,
    n.dbid as core_concept_id
FROM intelligence_dev.ai_centre_definition_library.bsa_bnf_snomed_mappings m
LEFT JOIN prod_dwh.analyst_primary_care.concept n 
    ON n.name = m.code_description
LEFT JOIN intelligence_dev.ai_centre_definition_library.bsa_bnf_hierarchy h
    ON h.code = m.definition_id
;