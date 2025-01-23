from pyconceptlibraryclient import Client
import pandas as pd
import yaml

def get_phenotype_codelist(
    phenotype_id: str,
    version_id: int,
    full_output: bool = False,
    format_for_db: bool = True,
    print_raw_output_to_file=False,
) -> pd.DataFrame:
    """
    For a given phenotype ID and version ID, returns the codes from all concepts belonging to the phenotype.

    :param phenotype_id: str
    :param version_id: int
    :param full_output: bool - If true, returns a dataframe which is the flattened full output of the API call (i.e. includes nested information on the
                           attributes and coding system. If false, returns a dataframe with just the code and description.)
    :param format_for_db: bool - If true, returns a dataframe with the columns expected by the Phenotype class
    :param print_raw_output_to_file: bool - If true, sends the RAW api output to a file (use for debugging)
    :return: pd.DataFrame
    """
    client = Client(public=True)
    try:
        codelist_api_return = client.phenotypes.get_codelist(
            phenotype_id, version_id=version_id
        )
        if codelist_api_return is None:
            raise ValueError(
                "API returned None for the given phenotype_id and version_id"
            )
    except ValueError as e:
        print(f"An error occurred: {e}")
        return pd.DataFrame()  # Return an empty DataFrame in case of error

    if print_raw_output_to_file:
        filename = f"{phenotype_id}_{version_id}_raw_output.yaml"
        with open(filename, "w") as f:
            yaml.dump(codelist_api_return, f)
        print(f"Written output to {filename}")

    if full_output:
        return pd.json_normalize(codelist_api_return, sep="_")
    else:
        if format_for_db:
            codes = {
                key: [codedict[value_key] for codedict in codelist_api_return]
                for key, value_key in {
                    "phenotype_id": "phenotype_id",
                    "phenotype_version": "phenotype_version_id",
                    "phenotype_name": "phenotype_name",
                    "concept_code": "concept_id",
                    "clinical_code": "code",
                    "code_description": "description",
                }.items()
            }
            codes["coding_system"] = [
                codedict["coding_system"]["name"] for codedict in codelist_api_return
            ]  # need a separate line for this as it is nested one layer deeper than the rest of the output
        else:
            codes = {
                "Code": [codedict["code"] for codedict in codelist_api_return],
                "Description": [
                    codedict["description"] for codedict in codelist_api_return
                ],
            }

    return pd.DataFrame(codes)


def get_phenotypelist_from_search_term(search_term: str) -> pd.DataFrame:
    """
    For a given search term, returns a dataframe with the phenotype_id and name of all phenotypes that match the search term.

    Note that you can use this system to search for phenotypes relating to a particular coding system (coding_system) - e.g. "SNOMED CT", "ICD-10", etc.
    :param search_term: str
    :return: pd.DataFrame
    """

    client = Client(public=True)

    try:
        search_results = client.phenotypes.get(search=search_term)
        if search_results is None:
            raise ValueError("API returned None for the given search_term")

        page = search_results.get("page")
        total_pages = search_results.get("total_pages")
        search_data = search_results.get("data")

        # Retrieve all pages for this search term
        while page < total_pages:
            page += 1
            next_page = client.phenotypes.get(search=search_term, page=page)
            search_data.extend(next_page.get("data"))

        df = pd.json_normalize(search_data, sep="_")
        return df[["phenotype_id", "name"]]

    except ValueError as e:
        print(f"An error occurred: {e}")
        return pd.DataFrame()


if __name__ == "__main__":
    combined_list = get_phenotypelist_from_search_term("asthma")

    # Display the combined codelist
    print(combined_list)

    # print(get_phenotype_codelist('PH189', version_id=378, full_output=True))