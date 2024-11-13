import os
import time
import pandas as pd
from dataclasses import dataclass
from functools import wraps
from typing import Callable, Literal, Optional
import requests

# Token endpoint url (see https://ontology.onelondon.online/)
_ONELONDON_OPENID_ENDPOINT = "https://ontology.onelondon.online/authorisation/auth/realms/terminology/protocol/openid-connect/token"

# Authoring and production endpoints url (see https://ontology.onelondon.online/)
_ONELONDON_AUTHOR_ENDPOINT = "https://ontology.onelondon.online/authoring/fhir/"
_ONELONDON_PROD_ENDPOINT = "https://ontology.onelondon.online/production1/fhir/"

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

    Attributes:
        client_id: client ID for the FHIR server as environmental variable
        client_secret: client secret for the FHIR server as environment variable
        endpoint: the endpoint URL for the FHIR server (default: OneLondon authoring endpoint)
        open_id_token_url: the URL for the OpenID token endpoint (default: OneLondon OpenID endpoint)

    Methods:
        retrieve_concept_codes_from_id: retrieves a list of concept codes from a value set ID
        retrieve_concept_codes_from_url: retrieves a list of concept codes from a value set URL
    """
    
    # class level attribute
    client_id = os.getenv("CLIENT_ID")
    client_secret = os.getenv("CLIENT_SECRET")    

    def __init__(
        self,
        endpoint_type: str = 'authoring',
        open_id_token_url: str = _ONELONDON_OPENID_ENDPOINT,
    ):
                
        if endpoint_type not in ['authoring', 'production']:
            raise ValueError("Invalid endpoint_type. Use 'authoring' or 'production'.")

        if endpoint_type == 'production':
            self.endpoint = _ONELONDON_PROD_ENDPOINT
        else:
            self.endpoint = _ONELONDON_AUTHOR_ENDPOINT

        self._open_id_token_url: str = open_id_token_url
        self._access_token: str
        self._access_token_expire_time: int

        self._initialise_access_token()

    def _initialise_access_token(self):
        self._access_token, self._access_token_expire_time = self._get_access_token()

    def _get_access_token(self) -> tuple[str, int]:
        # define request contents
        headers = {"Content-Type": "application/x-www-form-urlencoded"}

        data = {
            "grant_type": "client_credentials",
            "client_id": FHIRTerminologyClient.client_id,
            "client_secret": FHIRTerminologyClient.client_secret,
        }

        # Request access token
        try:
            response = requests.post(
                self._open_id_token_url, headers=headers, data=data
            )

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
            raise ValueError("Failed to retrieve access token.")

    @auto_refresh_token
    def retrieve_concept_codes_from_id(self, value_set_id: str) -> list[Optional[str]]:
        """
        Retrieves a list of concept codes that are found in a value set
            value_set_id: id of the target FHIR value set
        Returns a list of concept codes
        """

        url = f"{self.endpoint}ValueSet/{value_set_id}"

        headers = {"Authorization": f"Bearer {self._access_token}"}

        response = requests.get(url, headers=headers)

        # retrieve value set
        if response.status_code == 200:
            value_set = response.json()

            # extract list of codes
            concepts = (
                value_set.get("compose", {}).get("include", [])[0].get("concept", [])
            )
            code_list = [item.get("code", "no code listed") for item in concepts]

            return code_list
        else:
            print(f"Failed to retrieve data: {response.status_code} - {response.text}")
            return []

    @auto_refresh_token
    def retrieve_concept_codes_from_url(self, url: str) -> list[Optional[str]]:
        """
        Retrieves a list of concept codes that are found in a value set via a FHIR url
            url: contains the FHIR url
        Returns a list of concept codes
        """

        query_url = f"{self.endpoint}ValueSet/$expand?url={url}"

        headers = {"Authorization": f"Bearer {self._access_token}"}

        # retrieve value_set
        value_response = requests.get(query_url, headers=headers)

        if value_response.status_code == 200:
            value_set = value_response.json()

            try:
                code_list = [item['code'] for item in value_set.get('expansion', {}).get('contains', [])]
                return code_list
            except IndexError:
                print("No entries found in bundle.")
                return []
        else:
            print(
                f"Failed to retrieve value set: {value_response.status_code} - {value_response.text}"
            )
            return []

    @auto_refresh_token
    def retrieve_concept_names_from_url(self, url: str) -> list[Optional[str]]:
        """
        Retrieves a list of concept names that are found in a value set via a FHIR url
            url: contains the FHIR url
        Returns a list of concept names
        """

        query_url = f"{self.endpoint}ValueSet/$expand?url={url}"

        headers = {"Authorization": f"Bearer {self._access_token}"}

        # retrieve value_set
        value_response = requests.get(query_url, headers=headers)

        if value_response.status_code == 200:
            value_set = value_response.json()

            try:
                name_list = [item['display'] for item in value_set.get('expansion', {}).get('contains', [])]
                return name_list
            except IndexError:
                print("No entries found in bundle.")
                return []
        else:
            print(
                f"Failed to retrieve value set: {value_response.status_code} - {value_response.text}"
            )
            return []

    @auto_refresh_token
    def retrieve_refsets_from_megalith(self, url: str) -> Optional[pd.DataFrame]:
        """
        Retrieves all reference sets and their codesets that are found in a megalith.
            url: url of the megalith
        Returns a dataframe of refsets, ids, and codes (exploded)
        """

        query_url = f"{self.endpoint}ValueSet/$expand?url={url}"

        headers = {"Authorization": f"Bearer {self._access_token}"}

        # retrieve ref sets
        mega_response = requests.get(query_url, headers=headers)

        if mega_response.status_code == 200:
            megalith = mega_response.json()
            try:
                meganame = megalith.get('name')
                codeurl = megalith.get('url')

                display_list = [item['display'] for item in megalith.get('expansion', {}).get('contains', [])]                
                ref_list = [item['code'] for item in megalith.get('expansion', {}).get('contains', [])]
                
                code_column = []
                name_column = []

                for refset in ref_list:
                    ref_url = f'http://snomed.info/xsct/999000011000230102/version/20230705?fhir_vs=refset/{refset}'
                    
                    try:
                        code_list = self.retrieve_concept_codes_from_url(ref_url)    
                        code_column.append(code_list)
                    except Exception as e:
                        code_column.append(f'unable to retrieve: {e}')

                    try:
                        name_list = self.retrieve_concept_names_from_url(ref_url)    
                        name_column.append(name_list)
                    except Exception as e:
                        name_column.append(f'unable to retrieve: {e}')                        

                df = pd.DataFrame({'megalith': [meganame] * len(ref_list),
                                   'url': [codeurl] * len(ref_list),
                                   'refset_name': display_list,
                                   'refset_code': ref_list,
                                   'concept_name': name_column,
                                   'concept_code': code_column})
                
                df = df.explode(['concept_name', 'concept_code']).reset_index(drop='True')

                return df
            
            except IndexError:
                print("No entries found in bundle.")
                return []
        else:
            print(
                f"Failed to retrieve ref sets: {mega_response.status_code} - {mega_response.text}"
            )
            return []