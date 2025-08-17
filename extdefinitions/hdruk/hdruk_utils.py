"""
`hdruk_utils.py`
"""

from datetime import datetime
from typing import Dict, List, Literal

import pandas as pd
import yaml
from pyconceptlibraryclient import Client

from phenolab.utils.definition import Definition


class HDRUKLibraryClient:
    """
    Client for interacting with the HDR UK Phenotype Library.
    Provides methods to search and retrieve phenotype definitions.
    """

    def __init__(self, public=True):
        """
        Initialize the HDR UK Library client.

        Args:
            public (bool):
                Public access flag
        """
        try:
            self.client = Client(public=public)
        except Exception as e:
            raise RuntimeError(f"Failed to initialise the HDR UK Library client: {e}") from e

    def get_phenotype_codelist(
        self,
        phenotype_id: str,
        version_id: int,
        output_format: Literal["full", "db", "basic"] = "db",
        print_raw_output_to_file=False,
    ) -> pd.DataFrame:
        """
        For a given phenotype ID and version ID, returns the codes from all concepts belonging to
        the phenotype.

        :param phenotype_id: str
        :param version_id: int
        :param output_format: ["full", "db", "basic"]
            - If full, returns a dataframe which is the flattened full output of the API call (i.e.
            includes nested information on the attributes and coding system.
            - If db, returns a dataframe with the columns expected by the Phenotype class
            - If basic, returns just the code and description.)
        :param print_raw_output_to_file: bool - If true, sends the RAW api output to a file (use for
          debugging)
        :return: pd.DataFrame
        """
        try:
            codelist_api_return = self.client.phenotypes.get_codelist(
                phenotype_id, version_id=version_id
            )

            if codelist_api_return is None:
                raise ValueError(
                    f"API returned None for phenotype {phenotype_id} version {version_id}"
                )

            if print_raw_output_to_file:
                filename = f"{phenotype_id}_{version_id}_raw_output.yaml"
                with open(filename, "w") as f:
                    yaml.dump(codelist_api_return, f)
                print(f"Written output to {filename}")

            if output_format == "full":
                return pd.json_normalize(codelist_api_return, sep="_")
            elif output_format == "db":
                allowed_vocabularies = getattr(self, '_allowed_vocabularies', None)
                return self._format_codelist_for_db(codelist_api_return, phenotype_id, allowed_vocabularies)
            else:  # i.e. basic
                return self._format_codelist_basic(codelist_api_return)

        except Exception as e:
            print(f"Error retrieving codelist for phenotype {phenotype_id}: {e}")
            return pd.DataFrame()

    def _format_codelist_for_db(self,
                                codelist_api_return: List[Dict],
                                phenotype_id: str,
                                allowed_vocabularies: List[str] = None) -> pd.DataFrame:
        """
        Format codelist for database storage with PhenoLab Definition fields
        """
        try:
            if allowed_vocabularies:
                original_count = len(codelist_api_return)
                vocab_counts = {}
                for item in codelist_api_return:
                    vocab = item["coding_system"]["name"]
                    vocab_counts[vocab] = vocab_counts.get(vocab, 0) + 1

                codelist_api_return = [
                    item for item in codelist_api_return
                    if item["coding_system"]["name"] in allowed_vocabularies
                ]

                excluded_vocabs = [v for v in vocab_counts.keys() if v not in allowed_vocabularies]
                included_vocabs = [v for v in vocab_counts.keys() if v in allowed_vocabularies]

                if not codelist_api_return:
                    print(f"Definition {phenotype_id} - fully excluded (no allowed vocabularies)")
                    return pd.DataFrame()
                elif excluded_vocabs:
                    excluded_str = ", ".join([f"{v} ({vocab_counts[v]})" for v in excluded_vocabs])
                    included_str = ", ".join([f"{v} ({vocab_counts[v]})" for v in included_vocabs])
                    print(f"Definition {phenotype_id} - excluded: {excluded_str}, included: {included_str}")

            codes = {
                key: [codedict[value_key] for codedict in codelist_api_return]
                for key, value_key in {
                    "phenotype_id": "phenotype_id",
                    "phenotype_version": "phenotype_version_id",
                    "phenotype_name": "phenotype_name",
                    "code": "code",
                    "code_description": "description",
                    "codelist_id": "concept_id",
                    "codelist_name": "concept_name",
                    "codelist_version": "concept_version_id",
                }.items()
            }

            codes["vocabulary"] = [
                codedict["coding_system"]["name"] for codedict in codelist_api_return
            ]  # need a separate line for this as it is nested one layer deeper than
            # the rest of the output

            # add version datetime - parse from concept_history_date
            codes["version_datetime"] = [
                pd.to_datetime(codedict["concept_history_date"]) for codedict in codelist_api_return
            ]

            codes["phenotype_source"] = ["HDRUK"] * len(codes["code"])

            # Add PhenoLab Definition fields directly
            codes["definition_id"] = [phenotype_id] * len(codes["code"])
            codes["definition_source"] = ["HDRUK"] * len(codes["code"])
            codes["definition_name"] = codes["phenotype_name"]
            codes["definition_version"] = [str(v) for v in codes["phenotype_version"]]

            return pd.DataFrame(codes)
        except Exception as e:
            print(f"Error formatting codelist for database: {type(e).__name__}, {e}")
            return pd.DataFrame()

    def _format_codelist_basic(self, codelist_api_return: List[Dict]) -> pd.DataFrame:
        """
        Format codelist with just codes and descriptions
        """
        try:
            codes = {
                "Code": [codedict["code"] for codedict in codelist_api_return],
                "Description": [codedict["description"] for codedict in codelist_api_return],
            }
            return pd.DataFrame(codes)
        except Exception as e:
            print(f"Error formatting basic codelist: {e}")
            return pd.DataFrame()

    def get_phenotypelist_from_search_term(self, search_term: str.capitalize) -> pd.DataFrame:
        """
        For a given search term, returns a dataframe with the phenotype_id and name of all
        phenotypes that match the search term.

        Note that you can use this system to search for phenotypes relating to a particular coding
        system (coding_system) - e.g. "SNOMED CT", "ICD-10", etc.
        :param search_term: str
        :return: pd.DataFrame
        """
        try:
            search_results = self.client.phenotypes.get(search=search_term)
            if search_results is None:
                raise ValueError("API returned None for the given search_term")

            page = search_results.get("page")
            total_pages = search_results.get("total_pages")
            search_data = search_results.get("data")

            # Retrieve all pages for this search term
            while page < total_pages:
                page += 1
                next_page = self.client.phenotypes.get(search=search_term, page=page)
                if next_page and "data" in next_page:
                    search_data.extend(next_page.get("data"))

            df = pd.json_normalize(search_data, sep="_")
            return df[["phenotype_id", "name", "versions", "coding_system", "data_sources"]]

        except Exception as e:
            print(f"An error occurred: {e}")
            return pd.DataFrame()


def load_definitions():
    """
    Load HDRUK definition configurations from configuration file.
    """
    with open("hdruk_config.yml", 'r') as f:
        config = yaml.safe_load(f)
    return config['definitions']


def load_allowed_vocabularies():
    """
    Load allowed vocabularies from configuration file.
    """
    with open("hdruk_config.yml", 'r') as f:
        config = yaml.safe_load(f)
    return config['allowed_vocabularies']


def process_single_definition(definition_config, hdr_client=None, allowed_vocabularies=None):
    """
    Process a single HDRUK definition to create PhenoLab Definition objects

    Args:
        definition_config:
            Dictionary containing phenotype_id, version_id, and description
        hdr_client:
            Optional shared HDRUKLibraryClient instance
        allowed_vocabularies:
            Optional list of allowed vocabularies

    Returns:
        list:
            List of definition dictionaries ready for DataFrame creation
    """
    phenotype_id = definition_config['phenotype_id']
    version_id = definition_config['version_id']

    print(f"Processing HDRUK definition {phenotype_id} version {version_id}...")

    if hdr_client is None:
        hdr_client = HDRUKLibraryClient()

    if allowed_vocabularies:
        hdr_client._allowed_vocabularies = allowed_vocabularies

    codelist_df = hdr_client.get_phenotype_codelist(
        phenotype_id=phenotype_id, version_id=version_id, output_format="db"
    )

    if codelist_df.empty:
        print(f"No data returned for {phenotype_id}")
        return []

    definition = Definition.from_dataframe(codelist_df)
    definition.uploaded_datetime = datetime.now()

    return definition.aslist
