from pyconceptlibraryclient import Client # pip install git+https://github.com/SwanseaUniversityMedical/pyconceptlibraryclient.git@v1.0.0
import pandas as pd

def get_code_ids(search_term: str) -> pd.DataFrame:
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
  combined_codelist = get_code_ids('asthma')

  # Display the combined codelist
  print(combined_codelist)
