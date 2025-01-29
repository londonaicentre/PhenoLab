import zipfile
from io import BytesIO
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
from phmlondon.snow_utils import SnowflakeConnection
from base.phenotype import Code, Codelist, Phenotype, VocabularyType, PhenotypeSource
from base.load_tables import load_phenotypes_to_snowflake
from datetime import datetime

def transform_to_phenotype(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform BNF data into phenotype objects
    """
    phenotypes = []
    current_datetime = datetime.now()

    # gropu by paragraph = phenotype
    for (para_code, para_name), para_group in df.groupby(['BNF Paragraph Code', 'BNF Paragraph']):

        # group by subparagraph = codelist
        codelists = []
        for (subpara_code, subpara_name), subpara_group in para_group.groupby(
            ['BNF Subparagraph Code', 'BNF Subparagraph']):

            # codes from chemical substances
            codes = [
                Code(
                    code=row['BNF Chemical Substance Code'],
                    code_description=row['BNF Chemical Substance'],
                    code_vocabulary=VocabularyType.BNF
                )
                for _, row in subpara_group.iterrows()
            ]

            codelist = Codelist(
                codelist_id=str(subpara_code),
                codelist_name=subpara_name,
                codelist_vocabulary=VocabularyType.BNF,
                codelist_version="1.0",
                codes=codes
            )
            codelists.append(codelist)

        phenotype = Phenotype(
            phenotype_id=str(para_code),
            phenotype_name=para_name,
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

    zip_name = "20241101_bsa_bnf.zip"
    with zipfile.ZipFile(f"data/{zip_name}", 'r') as zip_ref:
        csv_name = zip_ref.namelist()[0]
        with zip_ref.open(csv_name) as csv_file:
            df = pd.read_csv(BytesIO(csv_file.read()))

    phenotype_df = transform_to_phenotype(df)
    phenotype_df.columns = phenotype_df.columns.str.upper()

    load_phenotypes_to_snowflake(
        snowsesh=snowsesh,
        df=phenotype_df,
        table_name="BSA_BNF_MAPPINGS"
    )

    snowsesh.session.close()

if __name__ == "__main__":
    main()