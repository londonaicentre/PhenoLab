"""
`onto_utils.py`
"""

import os
import time
from datetime import datetime
from functools import wraps
from typing import Callable, Optional

import pandas as pd
import requests
from tqdm import tqdm

from phenolab.utils.definition import (
    Code,
    Codelist,
    Definition,
    DefinitionSource,
    VocabularyType,
)

# Token endpoint url (see https://ontology.onelondon.online/)
_ONELONDON_OPENID_ENDPOINT = "https://ontology.onelondon.online/authorisation/auth/realms/terminology/protocol/openid-connect/token"

# Authoring and production endpoints url (see https://ontology.onelondon.online/)
_ONELONDON_AUTHOR_ENDPOINT = "https://ontology.onelondon.online/authoring/fhir/"
_ONELONDON_PROD_ENDPOINT = "https://ontology.onelondon.online/production1/fhir/"

# SNOMED Reference Sets are retrieved via FHIR value set containers (`/ValueSet/$expand?url=`)

def auto_refresh_token(func) -> Callable:
    """
    This function decorator checks if the access token has expired and refreshes it if necessary.
    """

    @wraps(func)
    def wrap(self, *args, **kwargs):
        if time.time() > self._access_token_expire_time:
            print("[INFO] Access token expired. Auto-refreshing...")
            self._initialise_access_token()
        return func(self, *args, **kwargs)

    return wrap


class FHIRTerminologyClient:
    """
    A client for querying FHIR terminology services, such as the OneLondon terminology server.
    """

    def __init__(
        self,
        endpoint_type: str = "authoring",
        open_id_token_url: str = _ONELONDON_OPENID_ENDPOINT,
        env_vars=None,
    ):
        if endpoint_type not in ["authoring", "production"]:
            raise ValueError("Invalid endpoint_type. Use 'authoring' or 'production'.")

        if endpoint_type == "production":
            self.endpoint = _ONELONDON_PROD_ENDPOINT
        else:
            self.endpoint = _ONELONDON_AUTHOR_ENDPOINT

        self.env_vars = env_vars or ["CLIENT_ID", "CLIENT_SECRET"]

        self._confirm_env_vars()
        self._client_id = os.getenv("CLIENT_ID")
        self._client_secret = os.getenv("CLIENT_SECRET")

        self._open_id_token_url: str = open_id_token_url
        self._access_token: str
        self._access_token_expire_time: int

        self._initialise_access_token()

    def _initialise_access_token(self):
        self._access_token, self._access_token_expire_time = self._get_access_token()

    def _confirm_env_vars(self):
        """
        Confirms that necessary env vars are set correctly.
        """
        for var in self.env_vars:
            if not os.getenv(var):
                print(f"Error: Environment variable {var} not set.")
                raise ValueError("Environment variables not set.")

    def _get_access_token(self) -> tuple[str, int]:
        # define request contents
        headers = {"Content-Type": "application/x-www-form-urlencoded"}

        data = {
            "grant_type": "client_credentials",
            "client_id": self._client_id,
            "client_secret": self._client_secret,
        }

        # Request access token
        try:
            response = requests.post(self._open_id_token_url, headers=headers, data=data)

            # check HTTP status code
            response.raise_for_status()

            # == Get access token ==
            # May fail if the response does not contain the expected keys
            # Likely as a result of incorrect client_id or client_secret
            # (Don't need to try / except this - we want a failure!)
            access_token: str = response.json()["access_token"]
            expiry_time: int = round(time.time()) + response.json()["expires_in"]

            return access_token, expiry_time

        except requests.RequestException as e:
            print(f"Unable to request: {e}")
            print("Check client_id or client_secret, or connectivity.")
            raise ValueError("Failed to retrieve access token.") from e

    def _expand_with_pagination(self, url: str, offset: int = 0, count: int = 10000) -> dict:
        """
        Helper method to expand ValueSet with pagination parameters
        Put in to handle very long reference sets
        """
        query_url = f"{self.endpoint}ValueSet/$expand?url={url}&offset={offset}&count={count}"
        headers = {"Authorization": f"Bearer {self._access_token}"}

        response = requests.get(query_url, headers=headers)
        response.raise_for_status()
        return response.json()

    ### === VALUE SET / REFERENCE SET RETRIEVAL ===

    @auto_refresh_token
    def list_megalith_refsets(self, megalith_url: str) -> Optional[pd.DataFrame]:
        """
        Lists all SNOMED reference sets in a megalith with their metadata (without expanding members).

        Args:
            megalith_url:
                url of megalith containing refsets

        Returns:
            pd.DataFrame:
                df with refset metadata (code, name, description, etc.)
        """
        query_url = f"{self.endpoint}ValueSet/$expand?url={megalith_url}"
        headers = {"Authorization": f"Bearer {self._access_token}"}

        response = requests.get(query_url, headers=headers)

        if response.status_code == 200:
            megalith = response.json()
            try:
                megalith_name = megalith.get("name", "Unknown")
                megalith_url_actual = megalith.get("url", megalith_url)

                # get all refsets from expansion
                contains = megalith.get("expansion", {}).get("contains", [])

                if not contains:
                    print("No refsets found in megalith")
                    return None

                refset_data = []
                for item in contains:
                    refset_data.append({
                        "megalith_name": megalith_name,
                        "megalith_url": megalith_url_actual,
                        "refset_code": item.get("code", ""),
                        "refset_name": item.get("display", ""),
                        "refset_system": item.get("system", ""),
                        "version": item.get("version", "")
                    })

                df = pd.DataFrame(refset_data)
                return df

            except Exception as e:
                print(f"Error parsing megalith response: {e}")
                return None
        else:
            print(f"Failed to retrieve megalith: {response.status_code} - {response.text}")
            return None

    @auto_refresh_token
    def retrieve_concepts_from_url(self, url: str) -> tuple[list[str], list[str]]:
        """
        Retrieves concept codes and names from a single value set with automatic pagination
        Returns tuple of (codes, names)
        """
        all_concepts = []
        offset = 0

        while True:
            try:
                response = self._expand_with_pagination(url, offset)
                expansion = response.get("expansion", {})
                concepts = expansion.get("contains", [])

                if not concepts:
                    break

                all_concepts.extend(concepts)
                total = expansion.get("total", len(concepts))

                if len(all_concepts) >= total:
                    break

                offset += len(concepts)

            except requests.RequestException as e:
                print(f"Failed to retrieve value set: {e}")
                break

        if not all_concepts:
            return [], []

        codes = [item.get("code", "") for item in all_concepts]
        names = [item.get("display", "") for item in all_concepts]
        return codes, names

    @auto_refresh_token
    def retrieve_refsets_from_megalith(
        self, url: str, name_filter: Optional[str] = None, refset_codes: Optional[list] = None
    ) -> Optional[pd.DataFrame]:
        """
        Retrieves all SNOMED reference sets from a megalith.
        Returns a dataframe of refsets, ids, and codes (exploded)
        """

        query_url = f"{self.endpoint}ValueSet/$expand?url={url}"
        headers = {"Authorization": f"Bearer {self._access_token}"}

        # retrieve ref sets
        mega_response = requests.get(query_url, headers=headers)

        if mega_response.status_code == 200:
            megalith = mega_response.json()
            try:
                meganame = megalith.get("name")
                codeurl = megalith.get("url")

                # get all refsets
                contains = megalith.get("expansion", {}).get("contains", [])

                # filter refsets if specific codes are provided
                if refset_codes:
                    contains = [
                        item
                        for item in contains
                        if item.get("code", "") in refset_codes
                    ]

                    if not contains:
                        raise ValueError(f"No refsets found matching codes: {refset_codes}")

                # filter refsets if name_filter is provided (legacy support)
                elif name_filter:
                    contains = [
                        item
                        for item in contains
                        if name_filter.lower() in item.get("display", "").lower()
                    ]

                    if not contains:
                        raise ValueError(f"No refsets found matching filter: '{name_filter}'")

                display_list = [item["display"] for item in contains]
                ref_list = [item["code"] for item in contains]

                code_column = []
                name_column = []

                for refset in tqdm(ref_list):
                    ref_url = f"{url}/{refset}"

                    try:
                        code_list, name_list = self.retrieve_concepts_from_url(ref_url)
                        code_column.append(code_list)
                        name_column.append(name_list)
                    except Exception as e:
                        code_column.append(f"unable to retrieve: {e}")
                        name_column.append(f"unable to retrieve: {e}")

                df = pd.DataFrame(
                    {
                        "megalith": [meganame] * len(ref_list),
                        "url": [codeurl] * len(ref_list),
                        "refset_name": display_list,
                        "refset_code": ref_list,
                        "concept_name": name_column,
                        "concept_code": code_column,
                    }
                )

                df = df.explode(["concept_name", "concept_code"]).reset_index(drop=True)

                return df

            except IndexError:
                print("No entries found in bundle.")
                return None
        else:
            print(
                f"Failed to retrieve ref sets: {mega_response.status_code} - {mega_response.text}"
            )
            return None


def transform_refsets_to_definitions(refsets_df, megalith_config):
    """
    Transform refsets dataframe into list of Definition objects
    """
    definitions = []
    current_datetime = datetime.now()

    # Extract version date from URL
    try:
        version_string = megalith_config["url"].split("version/")[1].split("?")[0]
        version_date = datetime.strptime(version_string, "%Y%m%d")
    except (IndexError, ValueError):
        version_date = current_datetime

    # Group by refset to create definitions
    for refset_code, refset_group in refsets_df.groupby("refset_code"):
        first_row = refset_group.iloc[0]

        # Parse refset name (remove "NHS Digital GP extraction - " prefix if present)
        refset_name = first_row["refset_name"]
        if refset_name.startswith("NHS Digital GP extraction - "):
            parsed_name = refset_name.replace("NHS Digital GP extraction - ", "")
        else:
            parsed_name = refset_name

        # Create list of code objects
        codes = [
            Code(
                code=row["concept_code"],
                code_description=row["concept_name"],
                code_vocabulary=VocabularyType.SNOMED,
            )
            for _, row in refset_group.iterrows()
        ]

        # Create Codelist object
        codelist = Codelist(
            codelist_id=refset_code,
            codelist_name=parsed_name,
            codelist_vocabulary=VocabularyType.SNOMED,
            codelist_version=megalith_config["url"],
            codes=codes,
        )

        # Create Definition object
        definition = Definition(
            definition_id=refset_code,
            definition_name=parsed_name,
            definition_version=megalith_config["url"],
            definition_source=DefinitionSource.ONTOSERVER,
            codelists=[codelist],
            version_datetime=version_date,
            uploaded_datetime=current_datetime,
        )
        definitions.append(definition)

    return definitions
