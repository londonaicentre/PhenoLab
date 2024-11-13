import zipfile
from io import BytesIO
import pandas as pd
from pathlib import Path

def convert_zip_csv_to_parquet(zip_name, pq_name):
    """
    Read a zipped CSV file into memory and converts to parquet
    Expects .zip to contain a single CSV file
    """
    
    # Read the zip file into memory
    with zipfile.ZipFile(f"data/{zip_name}", 'r') as zip_ref:
        csv_name = zip_ref.namelist()[0]
        
        with zip_ref.open(csv_name) as csv_file:
            df = pd.read_csv(BytesIO(csv_file.read()))
    
    df.to_parquet(f"data/{pq_name}")
    
    return df

if __name__ == "__main__":
    zip_name = "20241101_bsa_bnf.zip"
    pq_name = "20241101_bsa_bnf.parquet"
    
    df = convert_zip_csv_to_parquet(zip_name, pq_name)
    print(f"Converted {zip_name} to {pq_name}")
    print(f"File length: {len(df)}")