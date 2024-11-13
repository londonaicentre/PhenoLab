import zipfile
from io import BytesIO
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
from phmlondon.snow_utils import confirm_env_vars, create_snowflake_session, select_database_schema, load_pq_as_table, list_tables, query_to_df

def convert_zip_csv_to_parquet(zip_name, pq_name):
    """
    Read a zipped CSV file into memory and converts to parquet
    Expects .zip to contain a single CSV file (this is expected format from NHS BSA)
    """
    
    # Read the zip file into memory
    with zipfile.ZipFile(f"data/{zip_name}", 'r') as zip_ref:
        csv_name = zip_ref.namelist()[0]
        
        with zip_ref.open(csv_name) as csv_file:
            df = pd.read_csv(BytesIO(csv_file.read()))
    
    df.to_parquet(f"data/{pq_name}")
    
    return df

if __name__ == "__main__":
    load_dotenv()

    # Check that environmental variables exist
    env_vars = ["SNOWFLAKE_SERVER", "SNOWFLAKE_USER", "SNOWFLAKE_USERGROUP"]
    confirm_env_vars(env_vars)

    # Set up Snowflake connection
    session = create_snowflake_session()
    
    database = "INTELLIGENCE_DEV"
    schema = "AI_CENTRE_DEV"
    select_database_schema(session, database, schema)
    
    # Check schema connection
    list_tables(session, database, schema)

    zip_name = "20241101_bsa_bnf.zip"
    pq_name = "20241101_bsa_bnf.parquet"
    
    df = convert_zip_csv_to_parquet(zip_name, pq_name)
    print(f"Converted {zip_name} to {pq_name}")
    print(f"File length: {len(df)}")

    load_pq_as_table(session, f"data/{pq_name}", "BSA_BNF_LOOKUP", database, schema)    
