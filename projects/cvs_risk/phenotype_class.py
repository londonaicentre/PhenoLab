import pandas as pd
import pprint

class Phenotype:
    def __init__(self, df):
        if self.verify_data_structure(df):
            self.df = df
        else:
            raise ValueError("Data structure does not match expected format")
        self.phenotype_id = df['phenotype_id'].unique().tolist()
        self.phenotype_version = df['phenotype_version'].unique().tolist()
        if len(self.phenotype_id) > 1 or len(self.phenotype_version) > 1:
            raise ValueError("Trying to add more than one phenotype at once")
        print(f"Phenotype object initialised - ID is {self.phenotype_id[0]} and version is {self.phenotype_version[0]}")

    def verify_data_structure(self, df: pd.DataFrame) -> bool:
        """Check the colums in the dataframe match those we want to put in the SQL table"""
        print("Verifying data structure")
        return set(df.columns) == {
            "phenotype_id",
            "phenotype_version",
            "phenotype_name",
            "concept_code",
            "coding_system",
            "clinical_code",
            "code_description",
        }

    def show(self):
        pprint.pprint(self.df)