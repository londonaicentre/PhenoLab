class TableQueries:
    queries = {'cvs_risk_phenotypes':"""CREATE TABLE IF NOT EXISTS cvs_risk_phenotypes (
                record_id INTEGER AUTOINCREMENT PRIMARY KEY,
                phenotype_id VARCHAR(30) NOT NULL,
                phenotype_version VARCHAR(30),
                phenotype_name VARCHAR(50),
                concept_code VARCHAR(30),
                coding_system VARCHAR(30),
                clinical_code VARCHAR(50),
                code_description VARCHAR(255)
            )"""}
    
    