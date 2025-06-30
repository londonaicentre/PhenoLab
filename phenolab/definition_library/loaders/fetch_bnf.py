
from pathlib import Path
from typing import Iterator
import pandas as pd

def process_snomed_mappings(xlsx_files):
    """
    Combine and deduplicate mapping files
    """
    dfs = [pd.read_excel(f) for f in xlsx_files]
    combined = pd.concat(dfs)
    return combined.drop_duplicates(subset=["BNF Code", "SNOMED Code"])

def preprocess_bnf_data(file_path: str, mapping_files: Iterator[Path], output_path: str):

    with open(file_path) as csv_file:
            bnf_df = pd.read_csv(csv_file, 
                usecols=["BNF Chemical Substance", 
                        "BNF Subparagraph Code", 
                        "BNF Subparagraph", 
                        "BNF Chemical Substance Code"])
        
    chemical_substances = bnf_df[
        ["BNF Chemical Substance", "BNF Subparagraph Code", "BNF Subparagraph", "BNF Chemical Substance Code"]
    ].drop_duplicates()

    print("BNF dataframe created!")

    # process different years of BNF SNOMED mapping files
    mappings = process_snomed_mappings(mapping_files)

    mappings["Chemical Substance Code"] = mappings["BNF Code"].str[:9]

    # join mappings with BNF chemical substances
    joined_data = pd.merge(
        mappings,
        chemical_substances,
        left_on="Chemical Substance Code",
        right_on="BNF Chemical Substance Code",
    )
    joined_data = joined_data.dropna(subset=["SNOMED Code"])
    joined_data.to_csv(f"{output_path}.csv", index=False)

if __name__ == "__main__":
    mapping_files = Path("definition_library/loaders/data/bnf_to_snomed/").glob("*.xlsx")
    output_path = "definition_library/loaders/data/bnf_to_snomed/processed_bnf_data"

    preprocess_bnf_data("definition_library/loaders/data/bsa_bnf/20241101_1730476037387_BNF_Code_Information.csv", 
        mapping_files, output_path)

    print(f"BNF data preprocessed and saved to {output_path}.csv")