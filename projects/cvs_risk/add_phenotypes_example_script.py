from phenotype_class import Phenotype
from hdruk_library_api import get_phenotype_codelist
from database_magangers import LocalDatabaseManager, SnowflakeDatabaseManager
from pprint import pprint

#  PH24 / 48 Diabetes
dm_codes = get_phenotype_codelist('PH24', version_id=48)
diabetes = Phenotype(dm_codes)
diabetes.show()

dbmanager = LocalDatabaseManager('data/phenotype.db', 'cvs_risk_phenotypes')
# dbmanager = SnowflakeDatabaseManager('INTELLIGENCE_DEV', 'AI_CENTRE_DEV', 'cvs_risk_phenotypes')
dbmanager.add_phenotype(diabetes)
data = dbmanager.get_all_phenotypes()
for row in data:
    pprint(row)