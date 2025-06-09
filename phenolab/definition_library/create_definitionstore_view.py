"""
This script created the DEFINITIONSTORE view in Snowflake, which unifies various definition tables and includes 
datbase ID (DBID) mappings. It only needs to be run once to set up the view, and then only needs to be updated if there
is a change to the structure of the underlying tables or if new tables are added. As such, it is not called anywhere and 
should be run manually once when setting up PhenoLab on a new Snowflake instance.
"""
from snowflake.snowpark import Session

def create_definitionstore_view(session: Session, database: str = "INTELLIGENCE_DEV",
        schema: str = "AI_CENTRE_DEFINITION_LIBRARY"):
    """
    Creates unified view of all definition tables with DBID mappings
    Args:
        session (Session):
            Snowflake session
        database (str):
            Name of the database to create the view in
        schema (str):
            Name of the schema to create the view in
    """

    TABLES = [
        "AIC_DEFINITIONS",
        "BSA_BNF_SNOMED_MAPPINGS",
        "HDRUK_DEFINITIONS",
        "NEL_SEGMENT_DEFINITIONS",
        "NHS_GP_SNOMED_REFSETS",
        "OPEN_CODELISTS",]

    view_sql = f"""
    CREATE OR REPLACE VIEW {database}.{schema}.DEFINITIONSTORE AS
    WITH definition_union AS (
        {
        " UNION ALL ".join(
            f"SELECT *, '{table}' AS SOURCE_TABLE FROM {database}.{schema}.{table} WHERE CODE IS NOT NULL"
            for table in TABLES
        )
    }
    )
    SELECT
        p.*,
        c.DBID,
        CASE c.MAPPING_TYPE
            WHEN 'Core SNOMED' THEN c.DBID
            WHEN 'Non Core Mapped to SNOMED' THEN cm.CORE
            ELSE NULL
        END as CORE_CONCEPT_ID
    FROM definition_union p
    LEFT JOIN PROD_DWH.ANALYST_PRIMARY_CARE.CONCEPT c
        ON p.CODE = c.CODE
        AND p.VOCABULARY = c.SCHEME_NAME
    LEFT JOIN PROD_DWH.ANALYST_PRIMARY_CARE.CONCEPT_MAP cm
        ON c.DBID = cm.LEGACY
        AND c.MAPPING_TYPE = 'Non Core Mapped to SNOMED'
    """
    session.sql(view_sql).collect()
    print("Created DEFINITIONSTORE view with DBID mappings")

def main():
    session = Session.builder.config("connection_name", "nel_icb").create()
    create_definitionstore_view(
        session=session,
        database="INTELLIGENCE_DEV",
        schema="AI_CENTRE_DEFINITION_LIBRARY"
    )

if __name__ == "__main__":
    main()
