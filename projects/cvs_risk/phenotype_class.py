import pandas as pd
import pprint

class Phenotype:
    def __init__(self, df):
        if self.verify_data_structure(df):
            self.df = df
        else:
            raise ValueError("Data structure does not match expected format")
        print("Phenotype object initialised")

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