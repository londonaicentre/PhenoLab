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

def get_codelist_from_search_term(search_term: str) -> pd.DataFrame:
  client = Client(public=True)

  search_results = client.phenotypes.get(search=search_term)

  pheno = [item for item in search_results['data'] if search_term in item['name'].lower()]

  # Initialize an empty list to store codelists
  codelists = []

  # Loop over each phenotype_id and get the codelist
  for phenotype in pheno:
      phenotype_id = phenotype['phenotype_id']
      
      # Get codelist for each phenotype_id
      codelist = client.phenotypes.get_codelist(phenotype_id)
      
      # Add phenotype_id to the codelist data
      for item in codelist:
          item['phenotype_id'] = phenotype_id
      
      # Append the codelist to the list
      codelists.extend(codelist)

  # Convert the combined codelist into a pandas DataFrame
  return pd.DataFrame(codelists)

if __name__ == '__main__':
  # combined_codelist = get_codelist_from_search_term('asthma')

  # # Display the combined codelist
  # print(combined_codelist)

  print(get_phenotype_codelist('PH189', version_id=378, full_output=True))

