import marimo

__generated_with = "0.13.4"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    from dotenv import load_dotenv
    from phmlondon.snow_utils import SnowflakeConnection
    from phmlondon.feature_store_manager import FeatureStoreManager
    return FeatureStoreManager, SnowflakeConnection, load_dotenv


@app.cell
def _(SnowflakeConnection, load_dotenv):
    load_dotenv('.env') # very weirdly, marimo changes the default behaviour of load_dotenv to look next to pyproject.toml first rather than in the cwd, so need to specify - https://docs.marimo.io/guides/configuration/runtime_configuration/#env-files
    conn = SnowflakeConnection()
    return (conn,)


@app.cell
def _(FeatureStoreManager, conn):
    DATABASE = "INTELLIGENCE_DEV"
    SCHEMA = "AI_CENTRE_FEATURE_STORE"
    METADATASCHEMA = "AI_CENTRE_FEATURE_STORE_METADATA"
    feature_store_manager = FeatureStoreManager(conn, DATABASE, SCHEMA, METADATASCHEMA)
    return (feature_store_manager,)


@app.cell
def _(feature_store_manager):
    _fid = feature_store_manager.get_feature_id_from_table_name('BMI_FEATURES_v1')
    feature_store_manager.remove_latest_feature_version(feature_id=_fid)
    return


@app.cell
def _(feature_store_manager):
    with open('create_tables/bmi_features.sql') as _fid:
                _query = _fid.read()
                # data = conn.session.sql(sql).collect()

    feature_store_manager.add_new_feature(
            feature_name="BMI features",
            feature_desc="""
                BMI features for diabetes model in window period
                """,
            feature_format="Continuous",
            sql_select_query_to_generate_feature=_query, 
            existence_ok=True)
    return


@app.cell
def _(feature_store_manager):
    _fid = feature_store_manager.get_feature_id_from_table_name('PATIENTS_WITH_DIABETES_ALL_V1')

    # feature_store_manager.remove_latest_feature_version(_fid)

    with open('create_tables/patients_with_diabetes_all_v2.sql') as _fileid:
                _query = _fileid.read()

    feature_store_manager.update_feature(feature_id=_fid, new_sql_select_query=_query, change_description="Removed BMI features as these are now computed elsewhere")


    return


@app.cell
def _(feature_store_manager):
    _fid = feature_store_manager.get_feature_id_from_table_name('PATIENTS_WITH_2_HBA1C_GREATER_THAN_EQUAL_TO_48_V3')

    with open('create_tables/patients_with_2_successive_hba1c_greater_than_equal_to_48_date_v2.sql') as _fileid:
                _query = _fileid.read()

    feature_store_manager.update_feature(feature_id=_fid, new_sql_select_query=_query, change_description="Removing use of patient table as this added duplicates")
    return


@app.cell
def _(feature_store_manager):
    with open('create_tables/hba1c_prediction_model_all_features.sql') as _fileid:
                _query = _fileid.read()

    feature_store_manager.add_new_feature(
        feature_name="HBA1c model all features",
        feature_desc="""
            Combination of all tables for hba1c prediction model
            """,
        feature_format="Mixed",
        sql_select_query_to_generate_feature=_query, 
        existence_ok=True
    )
    return


@app.cell
def _(feature_store_manager):
    with open('create_tables/hba1c_prediction_model_all_features.sql') as _fileid:
                _query = _fileid.read()

    _fid = feature_store_manager.get_feature_id_from_table_name('HBA1c_MODEL_ALL_FEATURES_V1')
    feature_store_manager.update_feature(_fid, _query, change_description="Added in age at start of blinded period", overwrite=True)
    return


@app.cell
def _(feature_store_manager):
    _fid = feature_store_manager.get_feature_id_from_table_name('HBA1c_MODEL_ALL_FEATURES_V1')
    feature_store_manager.refresh_latest_feature_version(_fid)
    return


if __name__ == "__main__":
    app.run()
