## Project Structure

- `/extdefinitions` - Staging third party clinical definitions for upload via PhenoLab
  environments
- `/phenolab` - Source code for PhenoLab, an app for creating and import codelists for defining population segments

## Summary

PhenoLab is a Streamlit application for managing clinical definitions (e.g. codelists) in Snowflake. It simplifies versioning, maintenence, and comparison of many complex codelists, and supports standardisation of measurements (e.g. unit mapping, conversion, and QA) that are discovered from any measurement definition.

SNOMED, ICD10, and OPCS4 are currently supported vocabularies. PhenoLab is written for the OneLondon Secure Data Environment and is designed for compatibility with standard AI Centre data pipelines in Snowflake.