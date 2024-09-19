import os
import pandas as pd
from dotenv import load_dotenv
from scripts.helper import confirm_env_vars, create_snowflake_session, select_database_schema, load_pq_as_table, list_tables
import sys

def create_unioned_dbid_table(session, dbid_table_name):
    """
    Creates a DBID lookup table for the 2022 Cambridge co-morbidity score.
    """ 
    try:
        sql_query = """
        WITH UNIONED AS (
            SELECT 
                "ConditionID" AS CONDITIONID,
                "ConditionName"AS CONDITIONNAME,
                CAST("ConceptID" AS VARCHAR) AS CONCEPTCODE,
                "PrimaryTerm" AS PRIMARYTERM,
                71 AS SCHEME
            FROM INTELLIGENCE_DEV.AI_CENTRE_DEV.CAMBRIDGE_COMORB_2022_SNOMED
            UNION ALL
            SELECT
                "ConditionID" AS CONDITIONID,
                "ConditionName" AS CONDITIONNAME,
                CAST("ProductID" AS VARCHAR) AS CONCEPTCODE,
                "PrimaryTerm" AS PRIMARYTERM,
                71 AS SCHEME
            FROM INTELLIGENCE_DEV.AI_CENTRE_DEV.CAMBRIDGE_COMORB_2022_DMD
        )
        SELECT 
            U.*,
            C.DBID,
            C.NAME AS MAPPEDNAME
        FROM UNIONED U
        LEFT JOIN PROD_DWH.ANALYST_PRIMARY_CARE.CONCEPT C ON U.CONCEPTCODE = C.CODE AND U.SCHEME = C.SCHEME
        """

        session.sql(f"CREATE OR REPLACE TABLE INTELLIGENCE_DEV.AI_CENTRE_DEV.{dbid_table_name} AS ({sql_query})").collect()
        
        print(f"Table '{dbid_table_name}' created or replaced successfully.")
        
    except Exception as e:
        print(f"Error creating table: {e}")
        raise

def analyse_missing_dbid(session, dbid_table_name):
    """
    Summarises missing DBID post mapping
    """
    try:
        sql_query = f"""
        SELECT 
            CONDITIONNAME,
            COUNT(*) AS NUMBER_OF,
            SUM(CASE WHEN DBID IS NULL THEN 1 ELSE 0 END) AS MISSING_DBID_COUNT,
            (SUM(CASE WHEN DBID IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*)) AS MISSING_DBID_PERCENTAGE
        FROM INTELLIGENCE_DEV.AI_CENTRE_DEV.{dbid_table_name}
        GROUP BY CONDITIONNAME
        ORDER BY MISSING_DBID_PERCENTAGE DESC
        """
        
        result = session.sql(sql_query).collect()
        
        df = pd.DataFrame(result, columns=['CONDITIONNAME', 'NUMBER_OF', 'MISSING_DBID_COUNT', 'MISSING_DBID_PERCENTAGE'])
        
        df['MISSING_DBID_PERCENTAGE'] = df['MISSING_DBID_PERCENTAGE'].round(2).astype(str) + '%'
        
        return df
    
    except Exception as e:
        print(f"Error quantifying missing DBID: {e}")
        raise

def main():
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

    # Load codelists from 2022 paper (previously converted to parquet) 
    load_pq_as_table(session, "data/camb_comorb_score/pq/dm+d codes .parquet", "CAMBRIDGE_COMORB_2022_DMD", database, schema)
    load_pq_as_table(session, "data/camb_comorb_score/pq/EMIS medication codes.parquet", "CAMBRIDGE_COMORB_2022_EMISMED", database, schema)
    load_pq_as_table(session, "data/camb_comorb_score/pq/SNOMED clinical terms.parquet", "CAMBRIDGE_COMORB_2022_SNOMED", database, schema)

    # Create DBID table
    dbid_table_name = "CAMBRIDGE_COMORB_2022_DBID"
    create_unioned_dbid_table(session, dbid_table_name)

    list_tables(session, database, schema)

    # Check how many DBIDs failed to join
    missing = analyse_missing_dbid(session, dbid_table_name)
    print(missing)

if __name__ == "__main__":
    main()