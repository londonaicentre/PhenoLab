from src.phenotype import Phenotype
from phmlondon.hdruk_api import HDRUKLibraryClient
from database_magangers import LocalDatabaseManager, SnowflakeDatabaseManager
from pprint import pprint

codefetcher = HDRUKLibraryClient()
dm_codes = codefetcher.get_phenotype_codelist('PH24', version_id=48)
diabetes = Phenotype.from_dataframe(dm_codes)
diabetes.show()

dbmanager = LocalDatabaseManager('data/phenotype.db', 'cvs_risk_phenotypes')
# # dbmanager = SnowflakeDatabaseManager('INTELLIGENCE_DEV', 'AI_CENTRE_DEV', 'cvs_risk_phenotypes')
dbmanager.add_phenotype(diabetes)
data = dbmanager.get_all_phenotypes()
for row in data:
    pprint(row)

# #  PH24 / 48 Diabetes
# dm_codes = get_phenotype_codelist('PH24', version_id=48)
# diabetes = Phenotype(dm_codes)
# diabetes.show()

# dbmanager = LocalDatabaseManager('data/phenotype.db', 'cvs_risk_phenotypes')
# # dbmanager = SnowflakeDatabaseManager('INTELLIGENCE_DEV', 'AI_CENTRE_DEV', 'cvs_risk_phenotypes')
# dbmanager.add_phenotype(diabetes)
# data = dbmanager.get_all_phenotypes()
# for row in data:
#     pprint(row)