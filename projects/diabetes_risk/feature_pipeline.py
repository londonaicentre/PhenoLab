from dotenv import load_dotenv
from phmlondon.snow_utils import SnowflakeConnection
from phmlondon.feature_store_manager import FeatureStoreManager

def refresh_all_features():

    load_dotenv()
    conn = SnowflakeConnection()

    DATABASE = "INTELLIGENCE_DEV"
    SCHEMA = "AI_CENTRE_FEATURE_STORE"
    METADATASCHEMA = "AI_CENTRE_FEATURE_STORE_METADATA"
    feature_store_manager = FeatureStoreManager(conn, DATABASE, SCHEMA, METADATASCHEMA)

    feature_table_names = ['HBA1C_MODEL_ALL_FEATURES_V1', 
                           'BMI_FEATURES_V1',
                           'DIABETIC_OUTCOME_FLAGS_V1',
                           'HBA1C_FEATURES_V1',
                           'HBA1C_WITH_UNIT_REALLOCATION_V1',
                           'PATIENTS_DIAGNOSED_BY_HBA1C_BUT_NOT_CODED_V3',
                           'PATIENTS_WITH_2_HBA1C_GREATER_THAN_EQUAL_TO_48_V4',
                           'PATIENTS_WITH_DIABETES_ALL_V2',
                           'PATIENTS_WITH_DIABETES_RESOLUTION_CODE_V1',
                           'PATIENTS_WITH_NON_T1DM_CODES_ALL_INSTANCES_V1',
                           'PATIENTS_WITH_NON_T1DM_CODES_V1',
                            'PERSON_MASTER_INDEX_V1']
    
    for f in feature_table_names:
        feature_store_manager.refresh_latest_feature_version(feature_store_manager.get_feature_id_from_table_name(f))