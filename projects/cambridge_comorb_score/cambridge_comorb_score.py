import os
import pandas as pd
from dotenv import load_dotenv
from phmlondon.snow_utils import SnowflakeConnection
import sys

# DEFINE QUERIES

dbid_table_name = "CAMBRIDGE_COMORB_2022_DBID"

## join on OneLondon DBIDs to the uploaded concepts
query_dbid_union = f"""
    CREATE OR REPLACE TABLE INTELLIGENCE_DEV.AI_CENTRE_DEV.{dbid_table_name} AS (
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
    )
    """

## summarise missing DBIDs 
query_missing_dbid = f"""
        SELECT 
            CONDITIONNAME,
            COUNT(*) AS NUMBER_OF,
            SUM(CASE WHEN DBID IS NULL THEN 1 ELSE 0 END) AS MISSING_DBID_COUNT,
            (SUM(CASE WHEN DBID IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*)) AS MISSING_DBID_PERCENTAGE
        FROM INTELLIGENCE_DEV.AI_CENTRE_DEV.{dbid_table_name}
        GROUP BY CONDITIONNAME
        ORDER BY MISSING_DBID_PERCENTAGE DESC
        """

# SCRIPTS
def analyse_missing_dbid(session, dbid_table_name):
    """
    Summarises missing DBID post mapping
    """
    try:
        df = snowsesh.execute_query_to_df(query_missing_dbid)

        df['MISSING_DBID_PERCENTAGE'] = df['MISSING_DBID_PERCENTAGE'].round(2).astype(str) + '%'
        
        return df
    
    except Exception as e:
        print(f"Error quantifying missing DBID")
        raise e

if __name__ == "__main__":
    load_dotenv()

    snowsesh = SnowflakeConnection()
    snowsesh.use_database("INTELLIGENCE_DEV")
    snowsesh.use_schema("AI_CENTRE_DEV")
    
    snowsesh.list_tables()

    snowsesh.load_parquet_as_table(
        "data/pq/dm+d codes .parquet", 
        "CAMBRIDGE_COMORB_2022_DMD"
    )
    snowsesh.load_parquet_as_table(
        "data/pq/EMIS medication codes.parquet", 
        "CAMBRIDGE_COMORB_2022_EMISMED"
    )
    snowsesh.load_parquet_as_table(
        "data/pq/SNOMED clinical terms.parquet", 
        "CAMBRIDGE_COMORB_2022_SNOMED"
    )

    snowsesh.execute_query(query_dbid_union)

    snowsesh.list_tables()

    missing = analyse_missing_dbid(snowsesh, dbid_table_name)
    print(missing)

    snowsesh.session.close()