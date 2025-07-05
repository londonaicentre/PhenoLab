# Generates BASE_BNF_STATEMENT
# Contains distinct medication prescriptions with BNF subparagraph names
# Maps BNF references to human-readable BNF subparagraph descriptions

BASE_BNF_STATEMENT_SQL = """
SELECT DISTINCT
    m.PERSON_ID,
    m.CLINICAL_EFFECTIVE_DATE,
    m.BNF_REFERENCE,
    b."BNF Subparagraph Code" AS BNF_SUBPARAGRAPH_CODE,
    b."BNF Subparagraph" AS BNF_NAME
FROM PROD_DWH.ANALYST_PRIMARY_CARE.MEDICATION_STATEMENT m
LEFT JOIN INTELLIGENCE_DEV.AI_CENTRE_DEV.BSA_BNF_LOOKUP b
    ON m.BNF_REFERENCE = LEFT(b."BNF Subparagraph Code", 6)
WHERE m.BNF_REFERENCE IS NOT NULL
"""


def main():
   """
   Creates the BASE_BNF_STATEMENT table for NEL ICB warehouse in both prod and dev schemas.
   """
   from snowflake.snowpark import Session

   DATABASE = "INTELLIGENCE_DEV"
   SCHEMAS = ["AI_CENTRE_FEATURE_STORE", "PHENOLAB_DEV"]  # prod and dev
   CONNECTION_NAME = "nel_icb"

   try:
       session = Session.builder.config("connection_name", CONNECTION_NAME).create()

       for schema in SCHEMAS:
           print(f"Creating BASE_BNF_STATEMENT table in {DATABASE}.{schema}...")

           session.sql(f"""
               CREATE OR REPLACE TABLE {DATABASE}.{schema}.BASE_BNF_STATEMENT AS
               {BASE_BNF_STATEMENT_SQL}
           """).collect()

           print(f"BASE_BNF_STATEMENT table created successfully in {schema}.")

       print("All BASE_BNF_STATEMENT tables created successfully.")

   except Exception as e:
       print(f"Error creating BASE_BNF_STATEMENT table: {e}")
       raise e
   finally:
       session.close()


if __name__ == "__main__":
   main()