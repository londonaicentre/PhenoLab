"""
`fetch_ontoserver.py`

Fetches NHS Primary Care SNOMED reference sets, and metadata, from OneLondon terminology server
and maps them to PhenoLab Definition objects.

Fetching all reference sets from the entire megalith will take approximately 20 minutes.

This script fetches only reference sets whose names contain the filter terms in the config file.

@ Dr. Joe Zhang
@ Dr. Lawrence Adams
"""

from datetime import datetime

import pandas as pd
import yaml
from dotenv import load_dotenv
from onto_utils import FHIRTerminologyClient, transform_refsets_to_definitions


def load_config():
    with open("ontoserver_config.yml", "r") as f:
        return yaml.safe_load(f)



def main():
    """
    1. Loads megalith URL from config file
    2. Retrieves refset metadata and full definitions
    3. Maps to PhenoLab Definition objects
    4. Saves as two parquet files: metadata and full definitions
    """
    load_dotenv(override=True)
    config = load_config()
    megalith_config = config["megalith"]

    print(f"Fetching refsets from: {megalith_config['name']}")

    try:
        fhir_client = FHIRTerminologyClient(endpoint_type=megalith_config["endpoint_type"])

        # 1. Fetch refset metadata (without member codes)
        print("Fetching refset metadata...")
        metadata_df = fhir_client.list_megalith_refsets(megalith_config["url"])

        if metadata_df is not None:
            metadata_df['fetched_datetime'] = datetime.now()
            metadata_path = "nhs_snomed_refset_metadata.parquet"
            metadata_df.to_parquet(metadata_path, index=False)
            print(f"Refset metadata saved to {metadata_path}")
            print(f"Retrieved metadata for {len(metadata_df)} refsets")
        else:
            print("No metadata retrieved")

        # 2. Filter metadata based on name filters
        name_filters = megalith_config.get("refset_name_filters", [])
        name_exclusions = megalith_config.get("refset_name_exclusions", [])
        
        if (name_filters or name_exclusions) and metadata_df is not None:
            filtered_metadata = metadata_df.copy()
            
            # Apply inclusion filters
            if name_filters:
                filtered_metadata = filtered_metadata[
                    filtered_metadata['refset_name'].str.lower().str.contains('|'.join(name_filters), case=False, na=False)
                ]
                print(f"After inclusion filters: {len(filtered_metadata)} refsets matching {name_filters}")
            
            # Apply exclusion filters
            if name_exclusions:
                filtered_metadata = filtered_metadata[
                    ~filtered_metadata['refset_name'].str.lower().str.contains('|'.join(name_exclusions), case=False, na=False)
                ]
                print(f"After exclusion filters: {len(filtered_metadata)} refsets (excluded terms: {name_exclusions})")
            
            filtered_refset_codes = filtered_metadata['refset_code'].tolist()
            print(f"Final: {len(filtered_refset_codes)} refsets after all filters")
        else:
            filtered_refset_codes = None
            print("No filters applied - downloading all refsets")

        # 3. Fetch full refsets with all member codes
        print("Fetching full refsets with member codes...")
        refsets_df = fhir_client.retrieve_refsets_from_megalith(megalith_config["url"], refset_codes=filtered_refset_codes)

        if refsets_df is None or refsets_df.empty:
            print("No refsets retrieved")
            return

        print(f"Retrieved {len(refsets_df)} concept rows from {refsets_df['refset_code'].nunique()} refsets")

        # 4. Transform to Definition objects
        definitions = transform_refsets_to_definitions(refsets_df, megalith_config)
        print(f"Created {len(definitions)} definition objects")

        # 5. Save full definitions
        all_definitions = []
        for definition in definitions:
            all_definitions.extend(definition.to_dataframe().to_dict('records'))

        combined_df = pd.DataFrame(all_definitions)
        combined_df.columns = combined_df.columns.str.upper()

        definitions_path = "nhs_snomed_refset_definitions.parquet"
        combined_df.to_parquet(definitions_path, index=False)
        print(f"OntoServer definitions saved to {definitions_path}")

        print(f"Saved metadata file: {metadata_path} ({len(metadata_df)} refsets)")
        print(f"Saved definitions file: {definitions_path} ({len(combined_df)} codes)")

        return combined_df, metadata_df

    except Exception as e:
        print(f"Error fetching refsets: {e}")
        raise e


if __name__ == "__main__":
    main()
