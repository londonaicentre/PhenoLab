import sqlite3
from phenotype_class import Phenotype

class PhenotypeStorageManager:
    """Class to create a SQL database and add new phenotypes to it"""
    def __init__(self, database_filename: str):
        # eventually this will become a snowflake database connection
        self.conn = sqlite3.connect(database_filename)
        self._create_table()
        print('PhenotypeStorageManager class initialised')

    def _create_table(self):
        print('Create table method called')
        cursor = self.conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS phenotypes (
                record_id INTEGER PRIMARY KEY,
                phenotype_id VARCHAR(30) NOT NULL,
                phenotype_version VARCHAR(30),
                phenotype_name VARCHAR(50), 
                concept_code VARCHAR(30),
                coding_system VARCHAR(30),
                clinical_code VARCHAR(50),
                code_description VARCHAR(255)
            )
        ''')
        self.conn.commit()
        cursor.close
    
    def add_phenotype(self, phenotype: Phenotype):
        """Takes the data in a Phenotype object and adds it to the database"""
        phenotype.df.to_sql("phenotypes", self.conn, if_exists="append", index=False)
        print('Phenotype added to database')
        #  Note that currently the phenotype will be readded the database each time this method is called for the same phenotype
    
    def get_all_phenotypes(self) -> list[tuple]:
        cursor = self.conn.cursor()
        cursor.execute('SELECT * FROM phenotypes')
        rows = cursor.fetchall()
        cursor.close
        return rows #may need tidying - consider this as a TODO once snowflake connected 