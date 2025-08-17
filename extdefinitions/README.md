# External Definitions Fetching

Fetches clinical definitions from external sources and converts them to parquet files for PhenoLab.
These are pushed up to Snowflake during PhenoLab deployment.

## Pipelines

### HDR UK (`hdruk/`)
- `hdruk_config.yml` contains phenotype IDs and versions for retrieval
- Pipeline fetches codelists from HDR UK API.
- Retrieves only ICD10 / SNOMED vocabularies
- Outputs `hdruk_definitions.parquet`

### OpenCodelists (`opencodelists/`)
- `opencodelists_config.yml` contains list of OpenCodelists urls for retrieval
- Pipeline scrapes metadata from URLs and downloads codelists
- Outputs `opencodelists_definitions.parquet`

### NHS SNOMED (`ontoserver/`)
- `ontoserver_config.yml` defines FHIR terminology server address for NHS SNOMED Megalith
- Authenticates with FHIR server and fetches SNOMED refsets (+ metadata file)
- Outputs `nhs_snomed_refset_definitions.parquet` and `nhs_snomed_metadata.parquet`

## Usage

Update all sources:
```bash
./update.sh
```

Update specific sources:
```bash
./update.sh --hdruk
./update.sh --opencodelists
./update.sh --nhs-snomed
```

## Limitations

These pipelines are simple but brittle, and require manual maintenance:
- Config files are manually updated when new definitions are needed
- Only one canonical configuration is maintained, and deployed into Snowflake as a static copy
- Upstream changes (e.g. in HDRUK, OpenCodelists, Ontoserver) will break this pipeline
- Updates must be triggered manually