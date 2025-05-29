import pandas as pd
import numpy as np

def get_data_by_cohort(snowsesh, cohort_table_name):
    """
    Retrieves all data from a specified cohort table.

    Args:
        snowsesh (object): Database session object for executing queries.
        cohort_table_name (str): Fully qualified name of the cohort table.

    Returns:
        DataFrame: The retrieved cohort data.
    """

    query = f"SELECT * FROM {cohort_table_name}"

    try:
        df = snowsesh.execute_query_to_df(query)
        df.columns = df.columns.str.lower()
        print(f"Retrieved {len(df)} rows from cohort table: {cohort_table_name}")
        return df
    except Exception as e:
        print(f"Error retrieving cohort data from {cohort_table_name}: {e}")
        raise

def get_data_by_class(snowsesh, class_1, class_2, class_3):
    """
    Retrieves dataset.

    Args:
        snowsesh (object): Database session object for executing queries.
        class_1 (str): First class to filter by.
        class_2 (str): Second class to filter by.
        class_3 (str): Third class to filter by.

    Returns:
        DataFrame: The retrieved dataset.
    """

    chosen_classes = [class_1, class_2, class_3]
    classes_condition = ", ".join(f"'{cls}'" for cls in chosen_classes)

    query = f"""
    SELECT
        o.id AS order_id,
        o.person_id,
        o.medication_statement_id,
        d.order_name AS concept_name,
        c.name,
        d.drug,
        o.dose,
        o.quantity_value,
        o.quantity_unit,
        d.class,
        d.core_concept_id,
        o.core_concept_id AS concept,
        o.clinical_effective_date AS order_date,
        o.duration_days,
        s.clinical_effective_date AS statement_date,
        s.cancellation_date AS statement_enddate
    FROM
        prod_dwh.analyst_primary_care.medication_order o
    LEFT JOIN
        intelligence_dev.ai_centre_dev.drug_table_v3 d
        ON d.core_concept_id = o.core_concept_id
    LEFT JOIN
        prod_dwh.analyst_primary_care.concept c
        ON c.dbid = o.core_concept_id
    LEFT JOIN
        prod_dwh.analyst_primary_care.medication_statement s
        ON s.id = o.medication_statement_id
    WHERE d.class IN ({classes_condition})
    LIMIT 100000;
    """

    try:
        df = snowsesh.execute_query_to_df(query)
        df.columns = df.columns.str.lower()
        print(f"Retrieved {len(df)} rows with columns: {list(df.columns)}")
        return df
    except Exception as e:
        print(f"Error retrieving modeling data: {e}")
        raise

def add_demographic_data(snowsesh, df, join_col="person_id"):

    """ Bring in the dempgraphics table and joins to the current table
        Arguments:
                  snowsesh:
                    df = the dataframe oyu want to add demographic data to
                      join_cols = what you ant the join to be on
      """

    query = """
    SELECT
    p.person_id,
    p.date_of_birth,
    g.name AS gender,
    e.name AS ethnicity,
    i.england_imd_decile as imd

    FROM prod_dwh.analyst_primary_care.patient p

    LEFT JOIN prod_dwh.analyst_primary_care.patient_address pa
        ON pa.id = p.current_address_id

    LEFT JOIN prod_dwh.analyst_primary_care.concept g
        ON g.dbid = p.gender_concept_id

    LEFT JOIN prod_dwh.analyst_primary_care.concept e
        ON e.dbid = p.ethnic_code_concept_id

    LEFT JOIN intelligence_dev.ai_centre_external.imd2019london i
        ON pa.lsoa_2011_code = i.ls11cd
;
    """
    try:
        # Retrieve demographic data
        demo_df = snowsesh.execute_query_to_df(query)
        demo_df.columns = demo_df.columns.str.lower()  # Ensure column names are lowercase

        # Perform left join on person_id
        merged_df = df.merge(demo_df, on=join_col, how="left")

        print(f"Merged data: {len(merged_df)} rows with columns: {list(merged_df.columns)}")
        return merged_df

    except Exception as e:
        print(f"Error retrieving or merging demographic data: {e}")
        raise

def match_closest_compliance_date(df, snowsesh):
    """
    Fetches compliance data from Snowflake and matches each order to the closest compliance date.

    Arguments:
        df : DataFrame with prescription orders, containing 'person_id' and 'order_date'.
        snowsesh : Active Snowflake connection.

    Returns:
        df : Updated with 'closest_compliance_date' and 'medication_compliance'.
    """

    # Fetch compliance data from Snowflake
    query = """
       SELECT DISTINCT person_id,
                clinical_effective_date as compliance_date,
                CASE
                    WHEN core_concept_id = '119686' THEN 'good'
                    WHEN core_concept_id = '239913' THEN 'poor'
                END AS medication_compliance
        FROM prod_dwh.analyst_primary_care.observation
        WHERE core_concept_id IN ('119686', '239913')
        ORDER BY person_id, compliance_date;
    """
    try:
        compliance_df = snowsesh.execute_query_to_df(query)
        compliance_df.columns = compliance_df.columns.str.lower()
        print(f"Retrieved {len(compliance_df)} rows with columns: {list(compliance_df.columns)}")

        # Ensure dates are in datetime format
        df["start_date"] = pd.to_datetime(df["start_date"], errors="coerce")
        compliance_df["compliance_date"] = pd.to_datetime(compliance_df["compliance_date"],
                                                          errors="coerce")

        # Merge compliance data (many-to-many merge)
        merged_df = df.merge(compliance_df, on="person_id", how="left")

        # Compute the absolute difference between start_date and compliance_date
        merged_df["date_diff"] = (merged_df["start_date"] - merged_df["compliance_date"]).abs()

        # Drop rows with NaN in 'date_diff', since we can't compute the closest date for these
        merged_df = merged_df.dropna(subset=["date_diff"])

        # Find the closest compliance date per order
        closest_compliance = merged_df.loc[
            merged_df.groupby(["person_id", "start_date"])["date_diff"].idxmin(),
                                           ["person_id",
                                            "start_date",
                                            "compliance_date",
                                            "medication_compliance"]
                                            ]

        # Merge back to orders_df
        df = df.merge(closest_compliance, on=["person_id", "start_date"], how="left")

        # Rename for clarity
        df.rename(columns={"compliance_date": "closest_compliance_date"}, inplace=True)

        return df

    except Exception as e:
        print(f"Error retrieving modeling data: {e}")
        raise

def agg_data_person_drug(df):
    """
    Aggregates data by person and drug, calculating the required metrics such as
    min start date, max start date, sum of covered days, sum of exposed days,
    and other pre-calculated values.

    Args:
        df (DataFrame): Input dataframe that should contain columns such as
                         'person_id', 'drug_name', 'order_date', 'covered_days',
                         'static_pdc', 'dynamic_pdc', 'medication_compliance',
                         'gender', 'ethnicity', 'imd', 'date_of_birth',
                         and 'class' (if available).

    Returns:
        DataFrame: Aggregated data with one row per person-drug combination.
    """

    # Ensure 'order_date' and 'date_of_birth' are in datetime format
    df['order_date'] = pd.to_datetime(df['order_date'])
    df['date_of_birth'] = pd.to_datetime(df['date_of_birth'])

    # Calculate age at start (first order date) - difference between order_date and date_of_birth
    df['age_at_order'] = df['order_date'].dt.year - df['date_of_birth'
                        ].dt.year - ((df['order_date'].dt.month < df['date_of_birth'].dt.month) |
                                ((df['order_date'].dt.month == df['date_of_birth'].dt.month) &
                                (df['order_date'].dt.day < df['date_of_birth'].dt.day))).astype(int)

    # Group by person_id and drug_name, then aggregate the required columns
    agg_df = df.groupby(['person_id', 'drug_name'], as_index=False).agg(
        min_start_date=('order_date', 'min'),
        max_start_date=('order_date', 'max'),
        medication_compliance=('medication_compliance', 'first'),
        compliance_date=('compliance_date', 'first'),  # corrected typo from 'complaince_date'
        gender=('gender', 'first'),
        ethnicity=('ethnicity', 'first'),
        imd=('imd', 'first'),
        drug_class=('class', 'first'),
        age_at_start=('age_at_order', 'first'),
        overall_inclusive_pdc=('overall_inclusive_pdc', 'first'),
        overall_exclusive_pdc=('overall_exclusive_pdc', 'first'),
        total_covered_days=('total_covered_days', 'first'),
        total_exposed_days=('total_exposure_days', 'first'),
        pre_inclusive_pdc=('pre_inclusive_pdc', 'first'),
        pre_exclusive_pdc=('pre_exclusive_pdc', 'first'),
        total_pre_covered_days=('total_pre_covered_days', 'first'),
        total_pre_exposure_days=('total_pre_exposure_days', 'first'),
        post_inclusive_pdc=('post_inclusive_pdc', 'first'),
        post_exclusive_pdc=('post_exclusive_pdc', 'first'),
        total_post_covered_days=('total_post_covered_days', 'first'),
        total_post_exposure_days=('total_post_exposure_days', 'first')
    )

    # Ensure imd and class_ are treated as categorical
    agg_df['imd'] = agg_df['imd'].astype(str).astype('category')
    agg_df['drug_class'] = agg_df['drug_class'].astype(str).astype('category')

    # Return the aggregated DataFrame
    return agg_df


def general_agg(df):
    """
    Aggregates data by person and drug, calculating the required metrics such as
    min start date, max start date, sum of covered days, sum of exposed days,
    and other pre-calculated values.

    Args:
        df (DataFrame): Input dataframe that should contain columns such as
                         'person_id', 'drug_name', 'order_date', 'covered_days',
                         'inclusive_pdc", 'exclusive_pdc'
                         'gender', 'ethnicity', 'imd', 'date_of_birth'.

    Returns:
        DataFrame: Aggregated data with one row per person-drug combination.
    """

    # Ensure 'order_date' and 'date_of_birth' are in datetime format
    df['order_date'] = pd.to_datetime(df['order_date'])
    df['date_of_birth'] = pd.to_datetime(df['date_of_birth'])

    # Calculate age at start (first order date) - difference between order_date and date_of_birth
    df['age_at_order'] = df['order_date'].dt.year - df['date_of_birth'
                        ].dt.year - ((df['order_date'].dt.month < df['date_of_birth'].dt.month) |
                                ((df['order_date'].dt.month == df['date_of_birth'].dt.month) &
                                (df['order_date'].dt.day < df['date_of_birth'].dt.day))).astype(int)

    # Group by person_id and drug_name, then aggregate the required columns
    agg_df = df.groupby(['person_id', 'drug_name'], as_index=False).agg(
        min_start_date=('order_date', 'min'),
        max_start_date=('order_date', 'max'),
        gender=('gender', 'first'),
        ethnicity=('ethnicity', 'first'),
        imd=('imd', 'first'),
        age_at_start=('age_at_order', 'first'),
        overall_inclusive_pdc=('overall_inclusive_pdc', 'first'),
        overall_exclusive_pdc=('overall_exclusive_pdc', 'first'),
        total_covered_days=('total_covered_days', 'first'),
        total_exposed_days=('total_exposure_days', 'first')
    )

    # Ensure imd and class_ are treated as categorical
    agg_df['imd'] = agg_df['imd'].astype(str).astype('category')

    # Return the aggregated DataFrame
    return agg_df


def attach_closest_results(
    df_person_drug: pd.DataFrame,
    df_results: pd.DataFrame,
    id_col: str = 'person_id',
    date_col_min: str = 'min_start_date',
    date_col_max: str = 'max_start_date',
    result_date_col: str = 'result_date',
    result_value_col: str = 'result_value',
    n_results: int = 3,
    window_days: int = 365
) -> pd.DataFrame:
    """
    Attach closest lab/test results within a time window around treatment dates.

    Pulls up to n_results within ±window_days of min and max treatment dates.

    Returns:
        A DataFrame with all original df_person_drug columns + 2n result columns.
    """

    final_rows = []

    df_results[result_date_col] = pd.to_datetime(df_results[result_date_col], errors='coerce')

    for _, row in df_person_drug.iterrows():
        person_id = row[id_col]
        min_date = pd.to_datetime(row[date_col_min])
        max_date = pd.to_datetime(row[date_col_max])

        person_results = df_results[df_results[id_col] == person_id].copy()

        # Results within ±window_days of min_start_date
        min_window_start = min_date - pd.Timedelta(days=window_days)
        min_window_end = min_date + pd.Timedelta(days=window_days)
        min_results = person_results[
            (person_results[result_date_col] >= min_window_start) &
            (person_results[result_date_col] <= min_window_end)
        ].copy()
        min_results['delta'] = (min_results[result_date_col] - min_date).abs().dt.days
        min_results = min_results.sort_values('delta').head(n_results).reset_index(drop=True)

        # Results within ±window_days of max_start_date
        max_window_start = max_date - pd.Timedelta(days=window_days)
        max_window_end = max_date + pd.Timedelta(days=window_days)
        max_results = person_results[
            (person_results[result_date_col] >= max_window_start) &
            (person_results[result_date_col] <= max_window_end)
        ].copy()
        max_results['delta'] = (max_results[result_date_col] - max_date).abs().dt.days
        max_results = max_results.sort_values('delta').head(n_results).reset_index(drop=True)

        # Build result row
        result_row = row.to_dict()
        for i in range(n_results):
            result_row[f'before_result_{i+1}'] = min_results.loc[i, result_value_col] if i < len(min_results) else np.nan
            result_row[f'after_result_{i+1}'] = max_results.loc[i, result_value_col] if i < len(max_results) else np.nan

        final_rows.append(result_row)

    return pd.DataFrame(final_rows)

def cohort_exclusions(df):
    """
    - Remove people without at least a year of medication
    - Remove people with missing bloods - need at least 1 before and 1 after for average
    """

    # Remove people without at least a year of medication and avoid SettingWithCopyWarning
    df = df[df['max_start_date'] - df['min_start_date'] >= pd.Timedelta(days=365)].copy()

    # Identify blood test columns
    before_cols = [col for col in df.columns if 'before_result' in col]
    after_cols = [col for col in df.columns if 'after_result' in col]

    # Drop rows where all before or all after results are null
    df = df[df[before_cols].notna().any(axis=1) & df[after_cols].notna().any(axis=1)]

    return df



def attach_closest_results_2(
    df_person_drug: pd.DataFrame,
    df_results: pd.DataFrame,
    id_col: str = 'person_id',
    date_col_min: str = 'min_start_date',
    date_col_max: str = 'max_start_date',
    result_date_col: str = 'result_date',
    result_value_col: str = 'result_value',
    n_results: int = 3
) -> pd.DataFrame:
    """
    Attach the 3 closest results only before min_start_date and only after max_start_date.

    No time window is used.
    """

    final_rows = []

    df_results[result_date_col] = pd.to_datetime(df_results[result_date_col], errors='coerce')

    for _, row in df_person_drug.iterrows():
        person_id = row[id_col]
        min_date = pd.to_datetime(row[date_col_min])
        max_date = pd.to_datetime(row[date_col_max])

        person_results = df_results[df_results[id_col] == person_id].copy()

        # Results strictly BEFORE min_start_date
        min_results = person_results[
            person_results[result_date_col] < min_date
        ].copy()
        min_results['delta'] = (min_results[result_date_col] - min_date).abs().dt.days
        min_results = min_results.sort_values('delta').head(n_results).reset_index(drop=True)

        # Results strictly AFTER max_start_date
        max_results = person_results[
            person_results[result_date_col] > max_date
        ].copy()
        max_results['delta'] = (max_results[result_date_col] - max_date).abs().dt.days
        max_results = max_results.sort_values('delta').head(n_results).reset_index(drop=True)

        # Build result row
        result_row = row.to_dict()
        for i in range(n_results):
            result_row[f'before_result_{i+1}'] = min_results.loc[i, result_value_col] if i < len(min_results) else np.nan
            result_row[f'after_result_{i+1}'] = max_results.loc[i, result_value_col] if i < len(max_results) else np.nan

        final_rows.append(result_row)

    return pd.DataFrame(final_rows)


def avg_results(df):
    """
    Adds 'before_avg' and 'after_avg' columns to the dataframe,
    calculated as the row-wise average of before and after result columns.

    Parameters:
        df (pd.DataFrame): The input dataframe containing the result columns.

    Returns:
        pd.DataFrame: The modified dataframe with new average columns.
    """
    df["before_avg"] = df[["before_result_1", "before_result_2", "before_result_3"]].mean(axis=1)
    df["after_avg"] = df[["after_result_1", "after_result_2", "after_result_3"]].mean(axis=1)
    df["result_diff"] = df["after_avg"] - df["before_avg"]

    return df
