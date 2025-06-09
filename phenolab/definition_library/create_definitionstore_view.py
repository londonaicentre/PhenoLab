"""
Script to update definitions from a particular source (HDRUK, GP refsets, BNF, NEL) - the source
is specified as an input
"""


from typing import Dict

from dotenv import load_dotenv
from snowflake.snowpark import Session
# from loaders.load_bnf_to_snomed import main as load_bsa_bnf_snomed
# from loaders.load_bsa_bnf import main as load_bsa_bnf
# #from loaders.load_hdruk import main as load_hdruk
# from loaders.load_nel_segments import main as load_nel
# from loaders.load_nhs_gp_snomed import main as load_snomed
#from loaders.load_open_codelists import main as load_open_codelists

from phmlondon.snow_utils import SnowflakeConnection

# LOADER_CONFIG = {
#     # 'hdruk': {
#     #     'func': load_hdruk,
#     #     'table': 'HDRUK_DEFINITIONS'
#     # },
#     'gpsnomed': {
#         #'func': load_snomed,
#         'table': 'NHS_GP_SNOMED_REFSETS'
#     },
#     'nelseg': {
#         #'func': load_nel,
#         'table': 'NEL_SEGMENT_DEFINITIONS'
#     },
#     'bsabnfsnomed': {
#         #'func': load_bsa_bnf_snomed,
#         'table': 'BSA_BNF_SNOMED_MAPPINGS'
#     },
#     'aicentre': {
#         'table': 'AIC_DEFINITIONS'
#     },
#     # 'opencodelists': {
#     #     'func': load_open_codelists,
#     #     'table': 'OPEN_CODELISTS'
#     # },
# }


def create_definitionstore_view(session: Session, database: str = "INTELLIGENCE_DEV",
        schema: str = "AI_CENTRE_DEFINITION_LIBRARY"):
    """
    Creates unified view of all definition tables with DBID mappings
    Args:
        snowsesh:
            Active Snowflake session
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


# def run_loaders(loader_flags: Dict[str, bool]):
#     """
#     Runs selected definition loaders and recreates unified view
#     Args:
#         loader_flags: dictionary of loader names + flags
#     """
#     load_dotenv()
#     snowsesh = SnowflakeConnection()
#     snowsesh.use_database("INTELLIGENCE_DEV")
#     snowsesh.execute_query("CREATE SCHEMA IF NOT EXISTS AI_CENTRE_DEFINITION_LIBRARY")
#     snowsesh.use_schema("AI_CENTRE_DEFINITION_LIBRARY")

#     try:
#         for loader_name, run_loader in loader_flags.items():
#             if run_loader and loader_name in LOADER_CONFIG:
#                 print(f"Running {loader_name} loader...")
#                 LOADER_CONFIG[loader_name]["func"]()
#                 print(f"{loader_name} loader completed")

#         # Always create view regardless of which loaders ran
#         create_definitionstore_view(snowsesh)

#     except Exception as e:
#         print(f"Error in loader execution: {e}")
#         raise e
#     finally:
#         snowsesh.session.close()


def main():
    load_dotenv(override=True)
    conn = SnowflakeConnection()
    create_definitionstore_view(
        session=conn.session,
        database="INTELLIGENCE_DEV",
        schema="AI_CENTRE_DEFINITION_LIBRARY"
    )

    # parser = argparse.ArgumentParser(description="Run definition loaders")

    # for loader_name in LOADER_CONFIG:
    #     parser.add_argument(f"--{loader_name}", action="store_true")

    # args = vars(parser.parse_args())
    # run_loaders(args)


if __name__ == "__main__":
    main()
