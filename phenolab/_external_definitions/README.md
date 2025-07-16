# External Definitions Fetching

This stage fetches external definition data and saves it to parquet files ready for upload.

## Scripts

- `fetch_hdruk.py`: Retrieves definitions from HDR UK Phenotype Library API
- `fetch_open_codelists.py`: Retrieves definitions from OpenCodelists website
- `fetch_bnf.py`: Processes BNF data and SNOMED mappings
- `scrape_open_codelists.py`: Utility for parsing OpenCodelists metadata
- `hdruk_api.py`: HDR UK API client utilities

## Usage

Run scripts from within the phenolab directory:

```bash
cd phenolab
python _external_definitions/fetch_hdruk.py
python _external_definitions/fetch_open_codelists.py
python _external_definitions/fetch_bnf.py
```

## Output

All scripts save processed definitions as parquet files in the `data/` subdirectories:
- `data/hdruk/hdruk_definitions.parquet`
- `data/open_codelists_compiled/open_codelists_definitions.parquet`
- `data/bnf_to_snomed/processed_bnf_data.parquet`

These parquet files are then consumed by the deployment stage.
