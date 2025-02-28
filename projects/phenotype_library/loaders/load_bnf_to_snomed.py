## prevents load from failing
import sys

## Must be run from update.py
import zipfile
from datetime import datetime
from io import BytesIO
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

from loaders.base.load_tables import load_phenotypes_to_snowflake
from loaders.base.phenotype import Code, Codelist, Phenotype, PhenotypeSource, VocabularyType
from phmlondon.snow_utils import SnowflakeConnection


def process_snomed_mappings(xlsx_files):
    """
    Combine and deduplicate mapping files
    """
    dfs = [pd.read_excel(f) for f in xlsx_files]
    combined = pd.concat(dfs)
    return combined.drop_duplicates(subset=['BNF Code', 'SNOMED Code'])

def build_phenotypes(zip_name, mapping_files):
    """
    Build phenotypes from BNF chemical substances and SNOMED mappings
    """
    # BSA BNF hierarchy
    with zipfile.ZipFile(f"loaders/data/{zip_name}", 'r') as zip_ref:
        csv_name = zip_ref.namelist()[0]
        with zip_ref.open(csv_name) as csv_file:
            bnf_df = pd.read_csv(BytesIO(csv_file.read()))
    chemical_substances = bnf_df[['BNF Chemical Substance',
                                  'BNF Chemical Substance Code']].drop_duplicates()

    # process different years of BNF SNOMED mapping files
    mappings = process_snomed_mappings(mapping_files)

    mappings['Chemical Substance Code'] = mappings['BNF Code'].str[:9]

    # join mappings with BNF chemical substances
    joined_data= pd.merge(
        mappings,
        chemical_substances,
        left_on='Chemical Substance Code',
        right_on='BNF Chemical Substance Code'
    )
    joined_data = joined_data.dropna(subset=['SNOMED Code'])
    phenotypes = []
    current_datetime = datetime.now()

    # group by chemical substance at phenotype level
    for (chem_code, chem_name), chem_group in joined_data.groupby(
        ['BNF Chemical Substance Code', 'BNF Chemical Substance']):

        # group by BNF Code at codelist level
        codelists = []
        for bnf_code, bnf_group in chem_group.groupby('BNF Code'):
            # create SNOMED codes for each mapping
            codes = [
                Code(
                    code=str(int(row['SNOMED Code'])),
                    code_description=row['DM+D: Product Description'],
                    code_vocabulary=VocabularyType.SNOMED
                )
                for _, row in bnf_group.iterrows()
            ]

            codelist = Codelist(
                codelist_id=bnf_code,
                codelist_name=bnf_group['BNF Name'].iloc[0],
                codelist_vocabulary=VocabularyType.SNOMED,
                codelist_version="1.0",
                codes=codes
            )
            codelists.append(codelist)

        phenotype = Phenotype(
            phenotype_id=chem_code,
            phenotype_name=chem_name,
            phenotype_version="1.0",
            phenotype_source=PhenotypeSource.NHSBSA,
            codelists=codelists,
            version_datetime=current_datetime,
            uploaded_datetime=current_datetime
        )
        phenotypes.append(phenotype)

    return pd.concat([p.to_dataframe() for p in phenotypes], ignore_index=True)

def main():
    load_dotenv()

    snowsesh = SnowflakeConnection()
    snowsesh.use_database("INTELLIGENCE_DEV")
    snowsesh.use_schema("AI_CENTRE_PHENOTYPE_LIBRARY")

    mapping_files = Path("loaders/data").glob("*.xlsx")
    phenotype_df = build_phenotypes("20241101_bsa_bnf.zip", mapping_files)
    phenotype_df.columns = phenotype_df.columns.str.upper()

    load_phenotypes_to_snowflake(
        snowsesh=snowsesh,
        df=phenotype_df,
        table_name="BSA_BNF_SNOMED_MAPPINGS"
    )

    snowsesh.session.close()

if __name__ == "__main__":
    print("ERROR: This script should not be run directly.")
    print("Please run from update.py using the appropriate flag.")
    sys.exit(1)
