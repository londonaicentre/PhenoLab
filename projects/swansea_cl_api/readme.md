Project to look into extracting phenotype libraries via existing APIs

# Existing phenotype libraries

## HDR UK Phenotype Library (code managed by the University of Swansea)
- [Website](https://phenotypes.healthdatagateway.org)
- [Python client for API](https://github.com/SwanseaUniversityMedical/pyconceptlibraryclient)
- [Phenotype search engine](https://conceptlibrary.saildatabank.com/phenotypes/) - includes links to relevant papers 
- ??[very similar but slightly different version of the documentation](https://conceptlibrary.saildatabank.com)
- [API explorer/documentation](https://phenotypes.healthdatagateway.org/api/v1/)

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