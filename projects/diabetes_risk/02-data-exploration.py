import marimo

__generated_with = "0.13.4"
app = marimo.App(width="full")


@app.cell
def _():
    """
    Initial feature list:
    - patients with diabetes
    - ?? active patients only
    - update create tables

    1. HBA1c
    2. age
    3. IMD
    4. gender
    5. ethnicity
    6. diabetic endpoints - amputation, cardiac disease, retinopathy, ketoacidosis, ESRF, gastroparesis, neuropathy, PVD/circulatory disorder
    7. medication - insulin, glp1, sulphonylurea
    8. medication compliance
    """
    return


@app.cell
def _():
    """
    Definitions made:

    - non type 1 diabetes
    - lower limb amputation
    - MI
    - stroke
    - heart failureÍ
    - retinopathy/eye disease
    - dka
    - renal disorder due to T2DM
    - neuropathy/gastroparesis
    - PVD

    definitions to do:
    [x] hba1c
    [ ] medications
    """
    return


@app.cell
def _():
    """
    UPDATED FEATURE LIST
    Need to do an hba1c diagnosis Vs code diagnosis at some point.

    Factors to predict future hba1c:
    - age at prediction point 
    - Insulin and compliance with it
    - Metformin and compliance 
    - Etc for other meds 
    - BMI
    - BMI increase rate
    - Last egfr
    - Sex
    - Ethnicity 
    - Age at first diabetic hba1c/diagnosis 
    - The various endpoints I defined as flags as binaries
    - Number of hba1cs?
    - Rate of increase?
    - Time since last hba1c
    - Most recent blood pressure
    - IMD
    - Diabetic medicine count
    - Overall drug count/polypharmacy?
    - BMI at first diagnosis?
    - Time since diagnosis
    - Rate of change in HbA1c in different time periods??
    - Time since last HbA1c?

    -> Should do outcomes (bad events, admissions) for the outcome period and show that hba1c correlates and can predict these.
    """
    return


@app.cell
def _():
    from dotenv import load_dotenv, dotenv_values
    from phmlondon.snow_utils import SnowflakeConnection
    from phmlondon.feature_store_manager import FeatureStoreManager
    import os
    import marimo as mo
    import pandas as pd
    import matplotlib.pyplot as plt
    return FeatureStoreManager, SnowflakeConnection, load_dotenv, plt


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
    # Create feature

    with open('create_tables/all_hba1c_with_custom_def.sql') as fid:
        query = fid.read()
        # data = conn.session.sql(sql).collect()

    feature_store_manager.add_new_feature(
        feature_name="HbA1c_with_unit_reallocation",
        feature_desc=""""
            HbA1cs selected using a custom definition which pulls out any SNOMED code in our database that looks like an Hba1c and where results have been allocated to a significant proportion of the entries. Results are cleaned - the ones that look like they may be old units are converted to mmol/mol (see units for more).
            """,
        feature_format="Continuous",
        sql_select_query_to_generate_feature=query, 
        existence_ok=True)
    return


@app.cell
def _():
    """
    HbA1c analysis:
    - how many HbA1cs do we have?
    - how many patients have a repeated HbA1c?
    - what's the avergae number of HbA1cs, average frequency, average time between? 
    - do HbA1cs tend to get better or worse and by how much and over how long?
    """
    return


@app.cell
def _(conn):
    # Explore HbA1cs
    hba1cs = conn.session.sql("""
        select PERSON_ID, RESULT_VALUE_CLEANED_AND_CONVERTED_TO_MMOL_PER_MOL
        from INTELLIGENCE_DEV.AI_CENTRE_FEATURE_STORE.HBA1C_WITH_UNIT_REALLOCATION_V1;
        """).to_pandas()

    return (hba1cs,)


@app.cell
def _(hba1cs):
    print(f"Number of HbA1c readings: {len(hba1cs) :,}")
    return


@app.cell
def _(hba1cs, plt):
    # Plot the histogram
    plt.figure(figsize=(10, 6))
    plt.hist(hba1cs['RESULT_VALUE_CLEANED_AND_CONVERTED_TO_MMOL_PER_MOL'], bins=20, edgecolor='black')
    plt.title('Distribution of HbA1c Values')
    plt.xlabel('HbA1c (%)')
    plt.ylabel('Frequency')
    plt.axvline(x=48, color='red', linestyle='dashed', linewidth=2, label='48 mmol/mol')
    plt.grid(True)
    plt.show()

    return


@app.cell
def _(conn):
    readings_per_person = conn.session.sql(
        """
        SELECT 
        COUNT(*) AS total_readings,
        COUNT(DISTINCT person_id) AS unique_people,
        COUNT(*) * 1.0 / COUNT(DISTINCT person_id) AS avg_readings_per_person
        FROM INTELLIGENCE_DEV.AI_CENTRE_FEATURE_STORE.HBA1C_WITH_UNIT_REALLOCATION_V1;
        """).to_pandas()
    print(readings_per_person)
    return


@app.cell
def _(hba1cs, plt):
    reading_counts = hba1cs['PERSON_ID'].value_counts()
    num_with_one = (reading_counts == 1).sum()
    print(f"{num_with_one:,} patients have exactly one HbA1c reading.")
    num_more_than_ten = (reading_counts >10).sum()
    print(f"{num_more_than_ten:,} patients have more than 10 HbA1c readings.")

    # Plot histogram of the counts
    plt.figure(figsize=(10, 6))
    plt.hist(reading_counts, bins=range(1, reading_counts.max() + 2), edgecolor='black')
    plt.title('Distribution of HbA1c Reading Counts per Patient')
    plt.xlabel('Number of HbA1c Readings')
    plt.ylabel('Number of Patients')
    plt.grid(True)
    plt.xlim(1, 40)
    plt.show()


    return


@app.cell
def _(conn):
    timelags = conn.session.sql("""
        WITH dated_diffs AS (
        SELECT
            person_id,
            CLINICAL_EFFECTIVE_DATE,
            LAG(CLINICAL_EFFECTIVE_DATE) OVER (
                PARTITION BY person_id 
                ORDER BY CLINICAL_EFFECTIVE_DATE
            ) AS prev_date
        FROM INTELLIGENCE_DEV.AI_CENTRE_FEATURE_STORE.HBA1C_WITH_UNIT_REALLOCATION_V1
    ),
    time_differences AS (
        SELECT
            person_id,
            DATEDIFF(DAY, prev_date, CLINICAL_EFFECTIVE_DATE) AS days_between
        FROM dated_diffs
        WHERE prev_date IS NOT NULL
    )
    SELECT
        person_id,
        AVG(days_between) AS avg_days_between_readings
    FROM time_differences
    GROUP BY person_id;""").to_pandas()


    return (timelags,)


@app.cell
def _(plt, timelags):
    plt.figure(figsize=(10, 6))
    plt.hist(timelags['AVG_DAYS_BETWEEN_READINGS'], bins=30, edgecolor='black')
    plt.title('Average Days Between HbA1c Readings per Patient')
    plt.xlabel('Average Days Between Readings')
    plt.ylabel('Number of Patients')
    plt.grid(True)
    plt.xlim(0, 4000)

    mean_val = timelags['AVG_DAYS_BETWEEN_READINGS'].mean()
    plt.axvline(mean_val, color='red', linestyle='dashed', linewidth=2, label=f'Mean = {mean_val:.0f} days')

    median_val = timelags['AVG_DAYS_BETWEEN_READINGS'].median()
    plt.axvline(median_val, color='green', linestyle='dashed', linewidth=2, label=f'Median = {median_val:.0f} days')

    plt.legend()

    plt.show()
    return


@app.cell
def _(timelags):
    # Filter for patients with an average of 2 or fewer days between readings
    patients_with_close_readings = timelags[timelags['AVG_DAYS_BETWEEN_READINGS'] <= 2]
    num_patients = patients_with_close_readings['PERSON_ID'].nunique()
    print(f"{num_patients:,} patients have an average of 2 or fewer days between HbA1c readings.")
    return


@app.cell
def _(timelags):
    timelags['AVG_DAYS_BETWEEN_READINGS'].describe()

    return


@app.cell
def _(plt, timelags):
    plt.figure(figsize=(10, 6))
    plt.hist(timelags['AVG_DAYS_BETWEEN_READINGS'], bins=range(0,10), edgecolor='black')
    plt.title('Average Days Between HbA1c Readings per Patient')
    plt.xlabel('Average Days Between Readings')
    plt.ylabel('Number of Patients')
    plt.grid(True)
    plt.xlim(0, 10)
    plt.show()
    return


@app.cell
def _(conn):
    hba1c_changes = conn.session.sql(
        """
        WITH hba1c_changes AS (
        SELECT
            person_id,
            CLINICAL_EFFECTIVE_DATE,
            result_value_cleaned_and_converted_to_mmol_per_mol as hba1c_value,
            LAG(hba1c_value) OVER (PARTITION BY person_id ORDER BY CLINICAL_EFFECTIVE_DATE) AS prev_hba1c_value,
            LAG(CLINICAL_EFFECTIVE_DATE) OVER (PARTITION BY person_id ORDER BY CLINICAL_EFFECTIVE_DATE) AS prev_date
        FROM INTELLIGENCE_DEV.AI_CENTRE_FEATURE_STORE.HBA1C_WITH_UNIT_REALLOCATION_V1
    )
    SELECT
        person_id,
        clinical_effective_date,
        hba1c_value,
        prev_hba1c_value,
        prev_date,
        DATEDIFF(DAY, prev_date, CLINICAL_EFFECTIVE_DATE) AS days_between,
        (hba1c_value - prev_hba1c_value) / (NULLIF(DATEDIFF(DAY, prev_date, CLINICAL_EFFECTIVE_DATE), 0)/365.25) AS hba1c_change_per_year
    FROM hba1c_changes
    WHERE prev_hba1c_value IS NOT NULL
    ORDER BY person_id, clinical_effective_date;
    """
    ).to_pandas()
    return (hba1c_changes,)


@app.cell
def _(hba1c_changes, plt):
    plt.figure(figsize=(10, 6))
    plt.hist(hba1c_changes["HBA1C_CHANGE_PER_YEAR"], bins=range(-10, 10), edgecolor='black')
    plt.title('Average HbA1c change between readings')
    plt.xlabel('HbA1c change between successive readings (mmol/mol/year)')
    plt.ylabel('Number of Patients')
    plt.grid(True)
    plt.show()
    # This is a heavily truncated part of the true distribution
    return


@app.cell
def _():
    # now need to repeat these analyses for only patients with diabetes
    # diagnosed by code
    # 2 x HbA1c results >48 (see https://cks.nice.org.uk/topics/diabetes-type-2/diagnosis/diagnosis-in-adults/)
    # include 'ever diagnosed' - i.e. don't remove if they get better. Rationale being (1) they might be on meds (2) we want the model to be able to predict people who get better
    # At least 1 year of registration
    return


@app.cell
def _(feature_store_manager):
    # Create feature

    with open('create_tables/patients_with_nont1dm_codes_all_codes.sql') as _fid:
        _query = _fid.read()
        # data = conn.session.sql(sql).collect()

    feature_store_manager.add_new_feature(
        feature_name="patients_with_non_t1dm_codes_all_instances",
        feature_desc=""""
            Patients who have a code matching the custom non-t1dm definition, including all the relevant codes
            """,
        feature_format="Categorical",
        sql_select_query_to_generate_feature=_query, 
        existence_ok=True)
    return


@app.cell
def _(feature_store_manager):
    # Create feature

    with open('create_tables/patients_with_nont1dm_codes.sql') as _fid:
        _query = _fid.read()
        # data = conn.session.sql(sql).collect()

    feature_store_manager.add_new_feature(
        feature_name="patients_with_non_t1dm_codes",
        feature_desc=""""
            Patients who have a code matching the custom non-t1dm definition, including all the relevant codes, grouped by patient
            """,
        feature_format="Mixed",
        sql_select_query_to_generate_feature=_query, 
        existence_ok=True)
    return


@app.cell
def _(feature_store_manager):
    with open('create_tables/patients_with_2_hba1c_greater_than_equal_to_48.sql') as _fid:
        _query = _fid.read()
        # data = conn.session.sql(sql).collect()

    feature_store_manager.add_new_feature(
        feature_name="patients_with_2_hba1c_greater_than_equal_to_48",
        feature_desc="""
            Patients who have 2 HbA1cs >= 48 as per custom HbA1c definition
            """,
        feature_format="Mixed",
        sql_select_query_to_generate_feature=_query,
        existence_ok=True)
    return


@app.cell
def _(conn):
    coded_patients = conn.session.sql("""SELECT * FROM
    INTELLIGENCE_DEV.AI_CENTRE_FEATURE_STORE.PATIENTS_WITH_NON_T1DM_CODES_V1;""").to_pandas()
    return (coded_patients,)


@app.cell
def _(conn):
    two_hba1cs = conn.session.sql("""SELECT * FROM 
    INTELLIGENCE_DEV.AI_CENTRE_FEATURE_STORE.PATIENTS_WITH_2_HBA1C_GREATER_THAN_EQUAL_TO_48_V1;
        """).to_pandas()
    return (two_hba1cs,)


@app.cell
def _(coded_patients, two_hba1cs):
    print(f"Number of coded patients: {len(coded_patients):,}")
    print(f"Number of patients with diabetic HbA1cs: {len(two_hba1cs):,}")
    return


@app.cell
def _(feature_store_manager):
    with open('create_tables/patients_diagnosed_by_hba1c_but_not_coded.sql') as _fid:
        _query = _fid.read()
        # data = conn.session.sql(sql).collect()

    feature_store_manager.add_new_feature(
        feature_name="patients_diagnosed_by_hba1c_but_not_coded",
        feature_desc="""
            Patients who have 2 HbA1cs >= 48 as per custom HbA1c definition but not coded as diabetic
            """,
        feature_format="Mixed",
        sql_select_query_to_generate_feature=_query,
        existence_ok=True)



    return


@app.cell
def _(conn):
    non_coded_patients = conn.session.sql("""SELECT * FROM INTELLIGENCE_DEV.AI_CENTRE_FEATURE_STORE.PATIENTS_DIAGNOSED_BY_HBA1C_BUT_NOT_CODED_V1;""").to_pandas()

    print(f"Number of patients with diabetic HbA1cs but not coded as having diabetes: {len(non_coded_patients):,}")
    return (non_coded_patients,)


@app.cell
def _(hba1cs, non_coded_patients, plt):
    filtered_hba1cs = hba1cs[hba1cs['PERSON_ID'].isin(non_coded_patients['PERSON_ID'])]

    plt.figure(figsize=(10, 6))
    plt.hist(filtered_hba1cs['RESULT_VALUE_CLEANED_AND_CONVERTED_TO_MMOL_PER_MOL'], bins=30, edgecolor='black')
    plt.title('HbA1c Values for Non-Coded Patients')
    plt.xlabel('HbA1c (mmol/mol)')
    plt.ylabel('Number of Readings')
    plt.grid(True)

    _mean_val = filtered_hba1cs['RESULT_VALUE_CLEANED_AND_CONVERTED_TO_MMOL_PER_MOL'].mean()
    plt.axvline(_mean_val, color='red', linestyle='dashed', linewidth=2, label=f'Mean = {_mean_val:.2f} mmol/mol')

    plt.legend()
    plt.show()
    return


@app.cell
def _(coded_patients, hba1cs, plt):
    hba1cs_coded_patients = hba1cs[hba1cs['PERSON_ID'].isin(coded_patients['PERSON_ID'])]

    plt.figure(figsize=(10, 6))
    plt.hist(hba1cs_coded_patients['RESULT_VALUE_CLEANED_AND_CONVERTED_TO_MMOL_PER_MOL'], bins=30, edgecolor='black')
    plt.title('HbA1c Values for Coded Patients')
    plt.xlabel('HbA1c (mmol/mol)')
    plt.ylabel('Number of Readings')
    plt.grid(True)

    _mean_val = hba1cs_coded_patients['RESULT_VALUE_CLEANED_AND_CONVERTED_TO_MMOL_PER_MOL'].mean()
    plt.axvline(_mean_val, color='red', linestyle='dashed', linewidth=2, label=f'Mean = {_mean_val:.2f} mmol/mol')

    plt.legend()
    plt.show()
    return


@app.cell
def _():
    """
    Based on the above, decided to make the HbA1c requirement stricter - need two successive HbA1cs >= 48 more than 2 weeks apart (to avoid repeat coded values counting)
    """
    return


@app.cell
def _(feature_store_manager):
    with open('create_tables/patients_with_2_successive_hba1c_greater_than_equal_to_48.sql') as _fid:
        _query = _fid.read()
        # data = conn.session.sql(sql).collect()

    feature_id = feature_store_manager.get_feature_id_from_table_name('PATIENTS_WITH_2_HBA1C_GREATER_THAN_EQUAL_TO_48_V1')
    feature_store_manager.update_feature(feature_id=feature_id, new_sql_select_query=_query, change_description="Now require two successive HbA1cs >=48 (and must be more than 2 weeks apart)")
    return


@app.cell
def _(conn):
    two_hba1cs_v2 = conn.session.sql("""SELECT * FROM 
    INTELLIGENCE_DEV.AI_CENTRE_FEATURE_STORE.PATIENTS_WITH_2_HBA1C_GREATER_THAN_EQUAL_TO_48_V2;
        """).to_pandas()
    return (two_hba1cs_v2,)


@app.cell
def _(coded_patients, two_hba1cs_v2):
    print(f"Number of coded patients: {len(coded_patients):,}")
    print(f"Number of patients with diabetic HbA1cs: {len(two_hba1cs_v2):,}")
    return


@app.cell
def _(feature_store_manager):
    with open('create_tables/patients_diagnosed_by_hba1c_but_not_coded_v2.sql') as _fid:
        _query = _fid.read()
        # data = conn.session.sql(sql).collect()

    _feature_id = feature_store_manager.get_feature_id_from_table_name('PATIENTS_DIAGNOSED_BY_HBA1C_BUT_NOT_CODED_V1')
    feature_store_manager.update_feature(feature_id=_feature_id, new_sql_select_query=_query, change_description="Now require two successive HbA1cs >=48 (and must be more than 2 weeks apart)")
    return


@app.cell
def _(conn):
    non_coded_patients_v2 = conn.session.sql("""SELECT * FROM INTELLIGENCE_DEV.AI_CENTRE_FEATURE_STORE.PATIENTS_DIAGNOSED_BY_HBA1C_BUT_NOT_CODED_V2;""").to_pandas()

    print(f"Number of patients with diabetic HbA1cs but not coded as having diabetes: {len(non_coded_patients_v2):,}")
    return (non_coded_patients_v2,)


@app.cell
def _(hba1cs, non_coded_patients_v2, plt):
    filtered_hba1cs_v2 = hba1cs[hba1cs['PERSON_ID'].isin(non_coded_patients_v2['PERSON_ID'])]

    plt.figure(figsize=(10, 6))
    plt.hist(filtered_hba1cs_v2['RESULT_VALUE_CLEANED_AND_CONVERTED_TO_MMOL_PER_MOL'], bins=30, edgecolor='black')
    plt.title('HbA1c Values for Non-Coded Patients')
    plt.xlabel('HbA1c (mmol/mol)')
    plt.ylabel('Number of Readings')
    plt.grid(True)

    _mean_val = filtered_hba1cs_v2['RESULT_VALUE_CLEANED_AND_CONVERTED_TO_MMOL_PER_MOL'].mean()
    plt.axvline(_mean_val, color='red', linestyle='dashed', linewidth=2, label=f'Mean = {_mean_val:.2f} mmol/mol')

    plt.legend()
    plt.show()
    return


@app.cell
def _():
    """
    Discussed with Jordan and found out there are SNOMED resolved codes for a particular condition. Checked the database and there are a few diabetes resolved codes but only 1 in use. Need to look at (a) patients with diabetes resolved codes (b) how many of the selected patients have diabetes resolved codes
    """
    return


@app.cell
def _(feature_store_manager):
    _fid = feature_store_manager.get_feature_id_from_table_name('PATIENTS_WITH_DIABETES_RESOLUTION_CODE_v1')
    feature_store_manager.delete_feature(feature_id=_fid)
    return


@app.cell
def _(feature_store_manager):
    with open('create_tables/patients_with_diabetes_resolution_code.sql') as _fid:
        _query = _fid.read()
        # data = conn.session.sql(sql).collect()

    feature_store_manager.add_new_feature(
        feature_name="patients_with_diabetes_resolution_code",
        feature_desc="""
            Patients who have a SNOMED code for diabetes resolved on their record
            """,
        feature_format="Mixed",
        sql_select_query_to_generate_feature=_query,
        existence_ok=True)
    return


@app.cell
def _():
    # Need to add dates to previous features
    return


@app.cell
def _(feature_store_manager):
    with open('create_tables/patients_with_2_successive_hba1c_greater_than_equal_to_48_date.sql') as _fid:
        _query = _fid.read()

    _featureid = feature_store_manager.get_feature_id_from_table_name('PATIENTS_WITH_2_HBA1C_GREATER_THAN_EQUAL_TO_48_V2')
    feature_store_manager.update_feature(feature_id=_featureid, new_sql_select_query=_query, change_description='Added date of most recent event and earliest event and the HbA1c value (second val)')
    return


@app.cell
def _(feature_store_manager):
    with open('create_tables/patients_diagnosed_by_hba1c_but_not_coded_v3.sql') as _fid:
        _query = _fid.read()

    _featureid = feature_store_manager.get_feature_id_from_table_name('PATIENTS_DIAGNOSED_BY_HBA1C_BUT_NOT_CODED_V2')
    feature_store_manager.update_feature(feature_id=_featureid, new_sql_select_query=_query, change_description='Updated underlying tables')
    return


@app.cell
def _(feature_store_manager):
    _featureid = feature_store_manager.get_feature_id_from_table_name('PATIENTS_WITH_NON_T1DM_CODES_V1')
    feature_store_manager.remove_latest_feature_version(_featureid)
    return


@app.cell
def _():
    # TODO
    # [x] which patients?
    # [x] what about patients coded as diabetic who are no longer diabetic?
    # [ ] once have sorted that out, need to join together the two tables to get all diabetic patients and whether they were diagnosed by code or found by us
    # [ ] Dan's tables - but Dan uses different HbA1c definition to me!
    # [ ] hba1c trajectories
    # [ ] final hba1c distribution
    # [ ] age - distribution and relationship to final hba1c
    # [ ] exclude certain hba1cs as per jordan
    # [ ] need to plot/examine the units issue in more detail
    # [x] discuss hba1c selection with dan
    # [ ] should probably round hba1cs to whole numbers to avoid false precision
    # [x] add marimo to requirements
    return


if __name__ == "__main__":
    app.run()
