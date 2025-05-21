from dotenv import load_dotenv
from phmlondon.snow_utils import SnowflakeConnection
from phmlondon.feature_store_manager import FeatureStoreManager
import inspect

def connect_to_feature_store() -> FeatureStoreManager:
    load_dotenv()
    conn = SnowflakeConnection()

    DATABASE = "INTELLIGENCE_DEV"
    SCHEMA = "AI_CENTRE_FEATURE_STORE"
    METADATASCHEMA = "AI_CENTRE_FEATURE_STORE_METADATA"
    return FeatureStoreManager(conn, DATABASE, SCHEMA, METADATASCHEMA)

def update_all_features():
    """
    Use this when underlying SQL files have changed
    Takes a long time to run as every single table is dropped and created from scratch
    """
    feature_store_manager = connect_to_feature_store()

    errors = []

    #  1. HbA1cs
    try:
        with open('create_tables/all_hba1c_with_custom_def.sql') as fid:
            query = fid.read()

        feature_store_manager.update_feature(
            feature_id=feature_store_manager.get_feature_id_from_table_name('HBA1C_FEATURES_V1'),
            new_sql_select_query=query,
            change_description="Added date restriction for erroneuous HbA1c values",
            overwrite=True)
    
    except Exception as e:
        errors.append(str(e))
    
    # 2. Coded patients
    try:
        with open('create_tables/patients_with_nont1dm_codes_all_codes.sql') as fid:
            query = fid.read()

        feature_store_manager.update_feature(
            feature_id=feature_store_manager.get_feature_id_from_table_name('PATIENTS_WITH_NON_T1DM_CODES_ALL_INSTANCES_V1'),
            new_sql_select_query=query,
            change_description="Initial version",
            overwrite=True)
    except Exception as e:
        errors.append(str(e))

    try:    
        with open('create_tables/patients_with_nont1dm_codes.sql') as fid:
            query = fid.read()

        feature_store_manager.update_feature(
            feature_id=feature_store_manager.get_feature_id_from_table_name('PATIENTS_WITH_NON_T1DM_CODES_V1'),
            new_sql_select_query=query,
            change_description="Initial version",
            overwrite=True)
    
    except Exception as e:
        errors.append(str(e))

    # 3. Non-coded diabetic patients
    try:
        with open("create_tables/patients_with_2_successive_hba1c_greater_than_equal_to_48_date_v2.sql", "r") as f:
            sql_select_query = f.read()

        feature_store_manager.update_feature(
            feature_id=feature_store_manager.get_feature_id_from_table_name("PATIENTS_WITH_2_HBA1C_GREATER_THAN_EQUAL_TO_48_V1"),
            new_sql_select_query=sql_select_query,
            change_description="Updated underlying HbA1c table to exclude HbA1c from certain date ranges",
            overwrite=True)
    
    except Exception as e:
        errors.append(str(e))

    try:
        with open("create_tables/patients_diagnosed_by_hba1c_but_not_coded_v3.sql", "r") as _f:
            sql_select_query = _f.read()
        
        feature_store_manager.update_feature(
            feature_id=feature_store_manager.get_feature_id_from_table_name("PATIENTS_DIAGNOSED_BY_HBA1C_BUT_NOT_CODED_V1"),
            new_sql_select_query=sql_select_query,
            change_description="Changed underlying tables",
            overwrite=True)
    
    except Exception as e:
        errors.append(str(e))

    # 4. Patients with diabetes resolution codes
    try:
        with open("create_tables/patients_with_diabetes_resolution_code.sql", "r") as _f:
            sql_select_query = _f.read()

            feature_store_manager.update_feature(
            feature_id=feature_store_manager.get_feature_id_from_table_name("PATIENTS_WITH_DIABETES_RESOLUTION_CODE_V1"),
            new_sql_select_query=sql_select_query,
            change_description="Initial version",
            overwrite=True)
    
    except Exception as e:
        errors.append(str(e))

    # 5. Patients with diabetes
    try:
        with open("create_tables/patients_with_diabetes_all_v2.sql", "r") as _f:
            sql_select_query = _f.read()

        feature_store_manager.update_feature(
            feature_id=feature_store_manager.get_feature_id_from_table_name("PATIENTS_WITH_DIABETES_ALL_V1"),
            new_sql_select_query=sql_select_query,
            change_description="Removed obesity classes",
            overwrite=True)
    
    except Exception as e:
        errors.append(str(e))

    # 6. Engineered HbA1c features
    try:
        with open('create_tables/hba1c_features.sql') as _fid:
            new_sql_select_query = _fid.read()

        feature_store_manager.update_feature(
            feature_id=feature_store_manager.get_feature_id_from_table_name('HBA1C_FEATURES_V1'),
            new_sql_select_query=new_sql_select_query,
            change_description="Changed underlying HbA1cs",
            overwrite=True  
        )

    except Exception as e:
        errors.append(str(e))

    # 7. Outcome flags
    try:
        with open('create_tables/diabetic_outcome_flags_timeperiod.sql') as _fid:
            new_sql_select_query = _fid.read()
        
        feature_store_manager.update_feature(
            feature_id=feature_store_manager.get_feature_id_from_table_name('DIABETIC_OUTCOME_FLAGS_TIMEPERIOD_V1'),
            new_sql_select_query=new_sql_select_query,
            change_description="Initial version",
            overwrite=True
        )
    except Exception as e:
        errors.append(str(e))

    # 8. BMI features
    try:
        with open('create_tables/bmi_features.sql') as _fid:
            new_sql_select_query = _fid.read()

        feature_store_manager.update_feature(
            feature_id=feature_store_manager.get_feature_id_from_table_name('BMI_FEATURES_V1'),
            new_sql_select_query=new_sql_select_query,
            change_description="Initial version",
            overwrite=True
        )
    except Exception as e:
        errors.append(str(e))

    # 9. Finally, collate everything into one table
    try:
        with open('create_tables/hba1c_prediction_model_all_features.sql') as _fid:
            new_sql_select_query = _fid.read()

        feature_store_manager.update_feature(
            feature_id=feature_store_manager.get_feature_id_from_table_name('HBA1C_MODEL_ALL_FEATURES_V1'),
            new_sql_select_query=new_sql_select_query,
            change_description="Underlying tables updated",
            overwrite=True
        )
    except Exception as e:
        errors.append(str(e))

    if errors:
        print("Some functions failed:")
        for error in errors:
            print(f"- {error}")
    else:
        print("All functions ran successfully.")     


def refresh_features(feature_table_names: list[str]):

    """
    Use this to rerun the same SQL e.g. underlying tables have changed
    Tables recreated
    """

    feature_store_manager = connect_to_feature_store()
    
    for f in feature_table_names:
        feature_store_manager.refresh_latest_feature_version(feature_store_manager.get_feature_id_from_table_name(f))

if __name__ == "__main__":

    """
    ***Choose poison here***
    """

    feature_table_names = ['HBA1C_MODEL_ALL_FEATURES_V1', 
                        'BMI_FEATURES_V1',
                        'DIABETIC_OUTCOME_FLAGS_TIMEPERIOD_V1',
                        'HBA1C_FEATURES_V1',
                        'HBA1C_WITH_UNIT_REALLOCATION_V1',
                        'PATIENTS_WITH_2_HBA1C_GREATER_THAN_EQUAL_TO_48_V4',
                        'PATIENTS_WITH_DIABETES_ALL_V2',
                        'PATIENTS_WITH_DIABETES_RESOLUTION_CODE_V1',
                        'PATIENTS_WITH_NON_T1DM_CODES_ALL_INSTANCES_V1',
                        'PATIENTS_WITH_NON_T1DM_CODES_V1',
                        'PERSON_MASTER_INDEX_V1']

    # feature_table_names = ['PATIENTS_WITH_NON_T1DM_CODES_V1']
        
    # refresh_features(feature_table_names)

    update_all_features()
