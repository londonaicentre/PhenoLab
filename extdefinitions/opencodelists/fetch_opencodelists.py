"""
`fetch_open_codelists.py`

Scrapes and fetches OpenCodelist definitions given url in config

@ Dr. Isobel Weinberg
@ Dr. Joe Zhang
"""

import pandas as pd
from opencodelists_utils import load_urls, process_single_url


def main():
    """
    1. Loads urls from config file `opencodelists_config.yml`
    2. Processes each to extract + map fields to PhenoLab definition model
    3. Combines into a single df + saves as parquet
    """
    urls = load_urls()
    all_definitions = []

    for url in urls:
        print(f"Processing {url}")
        try:
            definition_data = process_single_url(url)
            all_definitions.extend(definition_data)
            print(f"Retrieved definition with {len(definition_data)} codes")
        except Exception as e:
            print(f"Failed to process {url}: {e}")

    print('OpenCodelists definitions retrieved successfully')
    combined_df = pd.DataFrame(all_definitions)
    combined_df.columns = combined_df.columns.str.upper()

    path = "opencodelists_definitions.parquet"
    combined_df.to_parquet(path, index=False)
    print(f"Open Codelists definitions saved to {path}")

    return combined_df


if __name__ == "__main__":
    main()
