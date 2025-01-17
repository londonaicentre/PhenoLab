Project to predict risk of a cardiovascular event from primary care data

# Repository code
Example for calling the HDRUK API for a particular phenotype and creating a phenotype object from it:

~~~python
from phenotype_class import Phenotype
from hdruk_library_api import get_phenotype_codelist

#  PH24 / 48 Diabetes
dm_codes = get_phenotype_codelist('PH24', version_id=48)
diabetes = Phenotype(dm_codes)
diabetes.show()
~~~

Example for adding a phenotype object to the database:

~~~python
from phenotype_storage_manager import PhenotypeStorageManager
dbmanager = PhenotypeStorageManager('data/phenotype.db')
dbmanager.add_phenotype(diabetes)
~~~

Example for running a search on the API and returning the phenotypes that fit that search:
~~~python
from hdruk_library_api import get_phenotypelist_from_search_term
get_phenotypelist_from_search_term('diabetes')
~~~

# Existing phenotype libraries

Project to look into extracting phenotype libraries via existing APIs

## HDR UK Phenotype Library (code managed by the University of Swansea)
- Extensive, wide choice of phenotypes, close links to underlying publications, covers codes across coding systems and a single phenotype includes codes from multiple systems 
- API has already failed once when I've been using it due to their servers going down. Any model needs to cover for this possibility if phenotype definitions not stored locally. 
- [Website](https://phenotypes.healthdatagateway.org)
- [Python client for API](https://github.com/SwanseaUniversityMedical/pyconceptlibraryclient)
- [Phenotype search engine](https://conceptlibrary.saildatabank.com/phenotypes/) - includes links to relevant papers 
- ??[very similar but slightly different version of the documentation](https://conceptlibrary.saildatabank.com)
- [API explorer/documentation](https://phenotypes.healthdatagateway.org/api/v1/) and [also here](https://github.com/SwanseaUniversityMedical/concept-library/wiki/Concept-Library-API) and see also [here](https://phenotypes.healthdatagateway.org/about/hdruk_about_technical_details/)

## NHS England Terminology Server giving access to the Human Phenotype Ontology
- [Website](https://digital.nhs.uk/services/terminology-server)
- The NHSE terminology server includes the [Human Phenotype Ontology](https://hpo.jax.org)
    - Compared to the HDR UK phenotype library (tested by searching for 'asthma in both'): 
        - Fewer codes
        - Cross references to SMOMED CT codes but appears signficantly less comprehensive
        - Inclusion of mapping to underlying genes
- All other [HL7 FIHR code systems](https://build.fhir.org/ig/HL7/UTG/codesystems.html) are also included on the NHSE terminology server
- [Available APIs](https://ontology.nhs.uk/#api-endpoints)

# Useful links
- [SNOMED CT terms browser](https://termbrowser.nhs.uk/)
- [NHS ICD-10 browser](https://classbrowser.nhs.uk/#/)

# Guidelines



# Potential features of interest/to explore
- QRISK 2018 ([paper](https://www.bmj.com/content/357/bmj.j2099))
    - Age
    - Sex
    - Ethnicity
    - Postcode
    - Smoking status
    - Diabetes status
    - Angina or heart attack in a 1st degree relative <60?
    - Chronic kidney disease (stage 3, 4 or 5)?
    - Atrial fibrillation?
    - On blood pressure treatement?
    - Migraines?
    - RA?
    - SLE?
    - Severe mental illness?
    - On atypical antipsychotics?
    - Regular oral steroids?
    - A diagnosis of or treatment for erectile dysfunction?
    - Cholesterol/HDL ratio
    - Systolic blood pressure (mmHg)
    - Standard deviation of at least two most recent systolic blood pressure readings (mmHg)
    - BMI
- General ideas
    - Medication compliance - lipid-lowering medications, anti-hypertensives, anti-platelets
    - Previous events
    - Whether events recorded for blood pressure screening, obesity screening, etc - absence of recordings may also be predictive

# Mapping features of interest onto existing HDRUK phenotypes

| Feature | Relevant phenotypes | Notes |
|---------|-------------|--------|
| BP medication | PH1595 - Antihypertensive medication - list of BNF codes.<br> There is also a set of phenotypes relating to GPRD codes e.g. Antihypertensives, Ace Inhibitors, Calcium Channel blockers  | |
| Systolic BP | PH408 - Blood pressure measurement - Read Codes V2 and OXMIS codes <br> PH16 - Blood pressure - Read Codes V2 <br> PH1654 - Blood pressure screening - Emis codes and Read codes V2 | Presumably observation codes have a value attached? <br> Could engineer separate features for e.g. stage 1/stage 2 hypertension etc|
| Hypertension | Lots!  |  |
| Smoking status | PH350 - Smoking status - IDC10 codes, Read codes V2 - includes categorisation into never, ex or current smokers<br> PH676 - Current smoker (P18) - Read codes V2 |  |
| Diabetes status | Lots  |  |
| Lipid-lowering medications | PH1600 - Lipid-lowering medication - BNF codes <br>PH926 - Lipid lowering agents - GPRD product codes | |
| BMI | | There are codes for any BMI screening occuring, and codes for obesity, but I'm not sure if there is a code which captures BMI as a continuous variable - may need to be engineered from the existing phenotypes.|
| Raised cholesterol | e.g. PH51 - Raised total cholesterol - no code list but a rule for defining raised cholesterol. <br> Also PH1655 - Cholesterol screening | |

# Misc of interest

- [Report on studing cardiovascular risk factors in severe mental illness: data availability in EHRs](https://datamind.org.uk/wp-content/uploads/2024/06/Datamind-RB3_Report_Summary-SHORT-REPORT.pdf)