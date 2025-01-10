# pip install git+https://github.com/SwanseaUniversityMedical/pyconceptlibraryclient.git@v1.0.0

from pyconceptlibraryclient import Client
import pandas as pd
client = Client(public=True)


search_results = client.phenotypes.get(
  search='asthma')


asthma_pheno = [item for item in search_results if 'asthma' in item['name'].lower()]

# Initialize an empty list to store codelists
codelists = []

# Loop over each phenotype_id and get the codelist
for phenotype in asthma_pheno:
    phenotype_id = phenotype['phenotype_id']
    
    # Get codelist for each phenotype_id
    codelist = client.phenotypes.get_codelist(phenotype_id)
    
    # Add phenotype_id to the codelist data
    for item in codelist:
        item['phenotype_id'] = phenotype_id
    
    # Append the codelist to the list
    codelists.extend(codelist)

# Convert the combined codelist into a pandas DataFrame
combined_codelist = pd.DataFrame(codelists)

# Display the combined codelist
print(combined_codelist)

