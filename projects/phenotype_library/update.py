from dotenv import load_dotenv
import argparse
from phmlondon.snow_utils import SnowflakeConnection
from load_hdruk_phenotypes import main as load_hdruk
from load_nhs_gp_snomed import main as load_snomed
from nel_segments import main as load_nel
from load_bsa_bnf import main as load_bnf
from typing import Dict

LOADER_CONFIG = {
    'hdruk': {
        'func': load_hdruk,
        'table': 'HDRUK_PHENOTYPES'
    },
    'snomed': {
        'func': load_snomed,
        'table': 'NHS_GP_SNOMED_REFSETS'
    },
    'bsabnf': {
        'func': load_bnf,
        'table': 'BSA_BNF_MAPPINGS'
    },
    'nelseg': {
        'func': load_nel,
        'table': 'NEL_SEGMENT_PHENOTYPES'
    }
}

def create_phenostore_view(snowsesh):
    """
    Creates unified view of all phenotype tables
    Args:
        snowsesh:
            Active Snowflake session
    """
    view_sql = f"""
    CREATE OR REPLACE VIEW PHENOSTORE AS
    {' UNION ALL '.join(
        f"SELECT *, '{name}' as SOURCE_SYSTEM FROM {config['table']}"
        for name, config in LOADER_CONFIG.items()
    )}
    """
    snowsesh.execute_query(view_sql)
    print("Created PHENOSTORE view")

def run_loaders(loader_flags: Dict[str, bool]):
    """
    Runs selected phenotype loaders and recreates unified view
    Args:
        loader_flags: dictionary of loader names + flags
    """
    load_dotenv()

    snowsesh = SnowflakeConnection()
    snowsesh.use_database("INTELLIGENCE_DEV")
    snowsesh.use_schema("AI_CENTRE_PHENOTYPE_LIBRARY")

    try:
        for loader_name, run_loader in loader_flags.items():
            if run_loader and loader_name in LOADER_CONFIG:
                print(f"Running {loader_name} loader...")
                LOADER_CONFIG[loader_name]['func']()
                print(f"{loader_name} loader completed")

        # Always create view regardless of which loaders ran
        create_phenostore_view(snowsesh)

    except Exception as e:
        print(f"Error in loader execution: {e}")
        raise e
    finally:
        snowsesh.session.close()

def main():
    parser = argparse.ArgumentParser(description='Run phenotype loaders')

    for loader_name in LOADER_CONFIG:
        parser.add_argument(f'--{loader_name}', action='store_true')

    args = vars(parser.parse_args())
    run_loaders(args)

if __name__ == "__main__":
    main()