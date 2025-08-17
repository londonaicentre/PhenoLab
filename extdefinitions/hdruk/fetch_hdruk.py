"""
`fetch_hdruk.py`

Note that this will ONLY fetch ICD10, OPCS4, and SNOMED-CT codelists.
If a HDRUK definition contains other vocabularies, they will be excluded.
This depends on `allowed_vocabularies` in `hdruk_config.yml`

@ Dr. Isobel Weinberg
@ Dr. Joe Zhang
"""

import pandas as pd
from hdruk_utils import HDRUKLibraryClient, load_allowed_vocabularies, load_definitions, process_single_definition


def main():
    """
    1. Loads definitions from config file `hdruk_config.yml`
    2. Processes each to extract + map fields to PhenoLab definition model
    3. Combines into a single df + saves as parquet
    """
    definitions = load_definitions()
    allowed_vocabularies = load_allowed_vocabularies()
    all_definitions = []

    # shared client
    hdr_client = HDRUKLibraryClient()

    for definition_config in definitions:
        phenotype_id = definition_config['phenotype_id']
        print(f"Processing {phenotype_id}")
        try:
            definition_data = process_single_definition(definition_config, hdr_client, allowed_vocabularies)
            all_definitions.extend(definition_data)
            print(f"Retrieved definition with {len(definition_data)} codes")
        except Exception as e:
            print(f"Failed to process {phenotype_id}: {e}")

    print('HDRUK definitions retrieved successfully')
    combined_df = pd.DataFrame(all_definitions)
    combined_df.columns = combined_df.columns.str.upper()

    path = "hdruk_definitions.parquet"
    combined_df.to_parquet(path, index=False)
    print(f"HDR UK definitions saved to {path}")

    return combined_df


if __name__ == "__main__":
    main()
