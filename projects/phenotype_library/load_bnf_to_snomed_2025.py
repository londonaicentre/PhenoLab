from datetime import datetime
from pathlib import Path

import pandas as pd
from base.load_tables import load_phenotypes_to_snowflake
from base.phenotype import Code, Codelist, Phenotype, PhenotypeSource, VocabularyType
from dotenv import load_dotenv

from phmlondon.snow_utils import SnowflakeConnection

################################################################################################
# Source of mapping file:

    # {
    #     'name': 'NHSBSA BNF SNOMED Mapping 2025',
    #     'url': 'https://www.nhsbsa.nhs.uk/prescription-data/understanding-our-data/bnf-snomed-mapping'
    # },

################################################################################################

def transform_to_phenotype(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform BNF mapped to SNOMED codes data into phenotype objects.
    """
    phenotypes = []
    current_datetime = datetime.now()

    # group by bnf code
    for (bnf_code, bnf_name), bnf_group in df.groupby(['BNF Code', 'BNF Name']):

        # group by snomed code
        codes = [
            Code(
                code=row['SNOMED Code'],
                code_description=row['DM+D: Product Description'],
                code_vocabulary=VocabularyType.SNOMED
            )
            for _, row in bnf_group.iterrows()
        ]

        # Create a codelist
        codelist = Codelist(
            codelist_id=str(bnf_code),
            codelist_name=bnf_name,
            codelist_vocabulary=VocabularyType.SNOMED,  # is actually BNF but won't allow me
            codelist_version="1.0",
            codes=codes
        )

        # Create a phenotype
        phenotype = Phenotype(
            phenotype_id=str(bnf_code),
            phenotype_name=bnf_name,
            phenotype_version="1.0",
            phenotype_source=PhenotypeSource.NHSBSA,
            codelists=[codelist],
            version_datetime=current_datetime,
            uploaded_datetime=current_datetime
        )

        phenotypes.append(phenotype)

    # Convert phenotype objects to DataFrame (so we can upload to Snowflake)
    return pd.concat([p.to_dataframe() for p in phenotypes], ignore_index=True)


def main():
    load_dotenv()  # Load environment variables

    # Connect to Snowflake
    snowsesh = SnowflakeConnection()
    snowsesh.use_database("INTELLIGENCE_DEV")
    snowsesh.use_schema("AI_CENTRE_PHENOTYPE_LIBRARY")


# Define the file path
    xlsx_path = Path("data/20250120_bnf_snomed_mapping.xlsx")

# Load Excel file into Pandas DataFrame
    if xlsx_path.exists():
        df = pd.read_excel(xlsx_path)  # Use read_excel instead of read_csv
    else:
        raise FileNotFoundError(f"Excel file not found: {xlsx_path}")

    # Transform Data
    phenotype_df = transform_to_phenotype(df)
    phenotype_df.columns = phenotype_df.columns.str.upper()  # Ensure column names are uppercase

    # Load into Snowflake
    load_phenotypes_to_snowflake(
        snowsesh=snowsesh,
        df=phenotype_df,
        table_name="BNF_SNOMED_MAPPINGS_2025"
    )

    # Close Snowflake connection
    snowsesh.session.close()

if __name__ == "__main__":
    main()
