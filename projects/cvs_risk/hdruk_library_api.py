from pyconceptlibraryclient import Client
import pandas as pd

def get_phenotype_codelist(phenotype_id: str, version_id: int, full_output: bool=False) -> pd.DataFrame:
  """
  For a given phenotype ID and version ID, returns the codes from all concepts belonging to the phenotype.
  
  Args:
  phenotype_id: str
  version_id: int
  full_output: bool - If true, returns a dataframe which is the flattened full output of the API call (i.e. includes nested information on the
  attributes and coding system. If false, returns a dataframe with just the code and description.)
  """
  client = Client(public=True)

  try:
    codelist_api_return = client.phenotypes.get_codelist(phenotype_id, version_id=version_id)
    if codelist_api_return is None:
      raise ValueError("API returned None for the given phenotype_id and version_id")
  except Exception as e:
    print(f"An error occurred: {e}")
    return pd.DataFrame()  # Return an empty DataFrame in case of error

  if full_output:
    return pd.json_normalize(codelist_api_return, sep='_')
  else:
    codes = {"Code": [codedict['code'] for codedict in codelist_api_return], 
                   "Description": [codedict['description'] for codedict in codelist_api_return]}
 
  return pd.DataFrame(codes)

def get_phenotpelist_from_search_term(search_term: str) -> pd.DataFrame:
  client = Client(public=True)

  search_results = client.phenotypes.get(search=search_term)
  page = search_results.get('page')
  total_pages = search_results.get('total_pages')
  search_data = search_results.get('data')
  
  while page < total_pages:
    page += 1
    next_page = client.phenotypes.get(search=search_term, page=page)
    search_data.extend(next_page.get('data'))

  df = pd.json_normalize(search_data, sep='_')
  return df[['phenotype_id', 'name']]

# TODO: Add in handling of empty search

if __name__ == '__main__':
  combined_list = get_phenotypelist_from_search_term('asthma')

  # Display the combined codelist
  print(combined_list)

  # print(get_phenotype_codelist('PH189', version_id=378, full_output=True))

