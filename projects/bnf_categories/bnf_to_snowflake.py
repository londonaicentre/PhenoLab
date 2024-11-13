import zipfile
from io import BytesIO
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
from phmlondon.snow_utils import SnowflakeConnection

def convert_zip_csv_to_parquet(zip_name, pq_name):
    """
    Read a zipped CSV file into memory and converts to parquet
    Expects .zip to contain a single CSV file (this is expected format from NHS BSA)
    """
    with zipfile.ZipFile(f"data/{zip_name}", 'r') as zip_ref:
        csv_name = zip_ref.namelist()[0]
        
        with zip_ref.open(csv_name) as csv_file:
            df = pd.read_csv(BytesIO(csv_file.read()))
    
    df.to_parquet(f"data/{pq_name}")
    
    return df

if __name__ == "__main__":
    load_dotenv()

    snowsesh = SnowflakeConnection()
    snowsesh.use_database("INTELLIGENCE_DEV")
    snowsesh.use_schema("AI_CENTRE_DEV")
    
    snowsesh.list_tables()

    zip_name = "20241101_bsa_bnf.zip"
    pq_name = "20241101_bsa_bnf.parquet"
    
    df = convert_zip_csv_to_parquet(zip_name, pq_name)
    print(f"Converted {zip_name} to {pq_name}")
    print(f"File length: {len(df)}")

    snowsesh.load_parquet_as_table(f"data/{pq_name}", "BSA_BNF_LOOKUP")
    
    snowsesh.session.close()