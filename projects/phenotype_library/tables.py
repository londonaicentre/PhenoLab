queries = {'cvs_risk_phenotypes':"""CREATE TABLE IF NOT EXISTS cvs_risk_phenotypes (
                record_id INTEGER PRIMARY KEY AUTOINCREMENT,
                phenotype_id VARCHAR(30) NOT NULL,
                phenotype_version VARCHAR(30),
                phenotype_name VARCHAR(50),
                phenotype_source VARCHAR(50),
                code VARCHAR(30),
                code_description VARCHAR(255),
                vocabulary VARCHAR(30),
                codelist_id VARCHAR(50),
                codelist_name VARCHAR(255),
                codelist_version VARCHAR(50),
                omop_concept_id VARCHAR(100),
                version_datetime VARCHAR(100), 
                uploaded_datetime VARCHAR(100)
            )"""}