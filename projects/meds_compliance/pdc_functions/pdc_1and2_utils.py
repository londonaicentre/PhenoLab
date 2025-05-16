from datetime import timedelta

import matplotlib.pyplot as plt
import pandas as pd


def preprocess_orders(df):
    """ Fills missing duration values and generates covered days for each order. """
    df['days_supply'] = df['calculated_duration'].fillna(df['duration_days']).fillna(0).astype(int)
    print(df.head())

    # Expand covered days
    df['covered_days'] = df.apply(lambda row: pd.date_range(row['order_date'],
                                                            periods=row['days_supply']), axis=1)
    print(df.head())
    # Explode into individual covered days & reset index
    return df.explode('covered_days', ignore_index=True)[['person_id', 'drug', 'covered_days']]

def compute_time_pdc(df, start_date, end_date):
    """ Computes PDC for a given time window. """
    total_days = (end_date - start_date).days + 1
    covered_days = df[(df['covered_days'] >= start_date) & (df['covered_days'
                                                        ] <= end_date)]['covered_days'].nunique()
    return covered_days / total_days if total_days > 0 else 0

def calculate_moving_pdc(df, window_size='12M', step_size='1M'):
    """ Calculates moving window PDC for all patients & drugs. """
    results = []
    patients_drugs = df[['person_id', 'drug']].drop_duplicates()

    for _, row in patients_drugs.iterrows():
        patient, drug = row['person_id'], row['drug']
        subset = df[(df['person_id'] == patient) & (df['drug'] == drug)]

        # Define time range
        min_date, max_date = subset['covered_days'].min(), subset['covered_days'].max()
        start_dates = pd.date_range(start=min_date, end=max_date, freq=step_size)

        for start in start_dates:
            end = start + pd.DateOffset(months=int(window_size[:-1])) - pd.Timedelta(days=1)
            pdc = compute_time_pdc(subset, start, end)
            results.append({'person_id': patient, 'drug': drug, 'start_window': start,
                            'end_window': end, 'PDC': pdc})

    return pd.DataFrame(results)



def plot_pdc_trend(df_patient):
    """
    Plots PDC over time for a patient, showing each drug's trend on the same graph.
    Takes only the patient-specific dataframe.
    """
    # Get the unique drugs for this patient
    unique_drugs = df_patient['drug'].unique()

    # Create the plot
    plt.figure(figsize=(10, 5))

    # Loop through each drug and plot the PDC trend with different colors
    for idx, drug in enumerate(unique_drugs):
        drug_pdc = df_patient[df_patient['drug'] == drug]
        plt.plot(drug_pdc['start_window'], drug_pdc['PDC'], marker='o', linestyle='-',
                 label=drug, color=plt.cm.tab10(idx % 10))

    # Get the earliest start_date and latest end_date for the patient (across all drugs)
    overall_start_date = df_patient['start_date'].min()
    overall_end_date = df_patient['end_date'].max()

    # Add vertical dashed red lines for start and end dates
    plt.axvline(x=overall_start_date, color='red', linestyle='--',
                label=f'Start Date: {overall_start_date.date()}')
    plt.axvline(x=overall_end_date, color='red', linestyle='--',
                label=f'End Date: {overall_end_date.date()}')

    # Title with drug names
    drug_names_str = ', '.join(unique_drugs)
    person_id = df_patient["person_id"].iloc[0]
    plt.title(
        f'PDC Over Time for Patient {person_id} - Drugs: {drug_names_str}'
    )

    # Labels and Formatting
    plt.xlabel('Time (Start Window)')
    plt.ylabel('Proportion of Days Covered (PDC)')
    plt.legend(title='Drugs', loc='upper left', bbox_to_anchor=(1,1))
    plt.xticks(rotation=45)
    plt.grid(True)
    plt.show()

def poi_analysis(df, start_poi, end_poi, window_size='12M', step_size='1M'):
    """
    Filters the data based on a given Period of Interest (POI) and computes PDC for that period.

    Parameters:
    - df: The original dataframe containing medication order data.
    - start_poi: Start date of the period of interest (e.g., '2024-01-01').
    - end_poi: End date of the period of interest (e.g., '2024-12-31').
    - window_size: Window size for moving PDC calculation (e.g., '12M').
    - step_size: Step size for the moving window (e.g., '1M').

    Returns:
    - pdc_df: DataFrame with moving window PDC calculations, including first and
    last order dates for each person_drug combination.
    """

    # Convert start_poi and end_poi to datetime if they are not already
    start_poi = pd.to_datetime(start_poi)
    end_poi = pd.to_datetime(end_poi)

    # Step 1: Preprocess the orders to generate 'covered_days'
    covered_days_df = preprocess_orders(df)
    print(covered_days_df.head())

    # Step 2: Filter the data based on the POI
    df_poi = covered_days_df[(covered_days_df['covered_days'
                                    ] >= start_poi) & (covered_days_df['covered_days'] <= end_poi)]

    # Step 3: Calculate the moving window PDC for the filtered period
    pdc_df = calculate_moving_pdc(df_poi, window_size=window_size, step_size=step_size)

    # Step 4: Get first and last order dates for each person_drug combo
    first_last_order_dates = df.groupby(['person_id', 'drug']).agg(
        start_date=('order_date', 'min'),
        end_date = ('order_enddate', 'max')
    ).reset_index()

    # Step 5: Merge first and last order dates with the PDC data
    pdc_df = pdc_df.merge(first_last_order_dates, on=['person_id', 'drug'], how='left')

    return pdc_df


def compute_pdc_overall(df):
    """
    Calculates inclusive and exclusive PDC for each person-drug combo, 
    and merges the results back into the original DataFrame.

    Intervention: could be input of compliance status SNOMED code of good complinace or bad compliance.
    Inclusive PDC: total days covered / (max orderdate + last(duration_days) - min orderdate)
    - i.e inclusive of order overlaps
    Exclusive PDC: total DATES covered / (max orderdate + last(duration_days) - min orderdate)
    - i.e excludes overlapping order periods

    Args:
        df (DataFrame) of Medication table class

    Returns:
        DataFrame: Original DataFrame with static_pdc and dynamic_pdc columns added.
    """

    results = []

    for (person, drug), group in df.groupby(["person_id", "drug_name"]):
        group = group.sort_values("order_date")

        # Inclusive PDC: sum of covered_days / time between first and last order
        total_covered_days = group["covered_days"].sum(skipna=True)

        min_start = group["order_date"].min()
        max_end = (group["order_date"] + pd.to_timedelta(group["covered_days"], unit="D")).max()

        exposure_days = (max_end - min_start
                         ).days if pd.notnull(min_start) and pd.notnull(max_end) else None

        inclusive_pdc = (total_covered_days / exposure_days
                      ) if pd.notnull(exposure_days) and exposure_days > 0 else None

        # Dynamic PDC: avoid overlaps using unique daily dates
        covered_dates = set()

        for _, row in group.iterrows():
            if pd.notnull(row['order_date']) and pd.notnull(row['covered_days']):
                coverage_days = int(row['covered_days'])

                if pd.notnull(row.get('days_to_next_order')) and row['days_to_next_order'] > 0:
                    coverage_days = min(coverage_days, int(row['days_to_next_order']))

                daily_dates = [
                    row['order_date'].date() + timedelta(days=i)
                    for i in range(coverage_days)
                ]
                covered_dates.update(daily_dates)

        exclusive_pdc = len(covered_dates) / exposure_days if pd.notnull(exposure_days
                                                                ) and exposure_days > 0 else None

        results.append({
            'person_id': person,
            'drug_name': drug,
            'overall_inclusive_pdc':inclusive_pdc if inclusive_pdc is not None else None,
            'overall_exclusive_pdc': min(exclusive_pdc, 1.0) if exclusive_pdc is not None else None,
            'total_covered_days': total_covered_days,  # Add covered_days to results
            'total_exposure_days': exposure_days  # Add exposure_days to results
        })

    # Create summary PDC DataFrame
    pdc_df = pd.DataFrame(results)

    # Merge back to original DataFrame
    df = df.merge(pdc_df, on=['person_id', 'drug_name'], how='left')

    return df


def compute_pdc_intervals(df):
    """
    Calculates inclusive and exclusive PDC for each person-drug combo, pre and post intervention.
    The pre and post intervention periods are defined based on the compliance_date.

    Args:
        df (DataFrame) of Medication table class

    Returns:
        DataFrame: Original DataFrame with pre and post PDC columns added.
    """

    # Create interval column
    df["interval"] = df.apply(
        lambda row: "pre" if pd.notnull(row["compliance_date"]) and row["order_date"] <= pd.to_datetime(row["compliance_date"]) else "post",
        axis=1
    )

    results = []

    for (person, drug), group in df.groupby(["person_id", "drug_name"]):
        group = group.sort_values("order_date")

        interval_data = {}

        for interval in ["pre", "post"]:
            sub_group = group[group["interval"] == interval]

            if sub_group.empty:
                interval_data[f'{interval}_inclusive_pdc'] = None
                interval_data[f'{interval}_exclusive_pdc'] = None
                interval_data[f'total_{interval}_covered_days'] = 0
                interval_data[f'total_{interval}_exposure_days'] = 0
                continue

            start_date = sub_group["order_date"].min()
            end_date = sub_group["order_date"].max()

            total_covered_days = sub_group["covered_days"].sum(skipna=True)

            exposure_days = (end_date - start_date).days if pd.notnull(start_date) and pd.notnull(end_date) else None

            inclusive_pdc = (total_covered_days / exposure_days
                            ) if pd.notnull(exposure_days) and exposure_days > 0 else None

            # Exclusive PDC
            covered_dates = set()
            for _, row in sub_group.iterrows():
                if pd.notnull(row['order_date']) and pd.notnull(row['covered_days']):
                    coverage_days = int(row['covered_days'])

                    if pd.notnull(row.get('days_to_next_order')) and row['days_to_next_order'] > 0:
                        coverage_days = min(coverage_days, int(row['days_to_next_order']))

                    daily_dates = [
                        row['order_date'].date() + timedelta(days=i)
                        for i in range(coverage_days)
                    ]
                    covered_dates.update(daily_dates)

            exclusive_pdc = (len(covered_dates) / exposure_days
                            ) if pd.notnull(exposure_days) and exposure_days > 0 else None

            # Save data
            interval_data[f'{interval}_inclusive_pdc'] = inclusive_pdc
            interval_data[f'{interval}_exclusive_pdc'] = min(exclusive_pdc, 1.0) if exclusive_pdc is not None else None
            interval_data[f'total_{interval}_covered_days'] = total_covered_days
            interval_data[f'total_{interval}_exposure_days'] = exposure_days

        results.append({
            'person_id': person,
            'drug_name': drug,
            **interval_data
        })

    # Create summary PDC DataFrame
    pdc_df = pd.DataFrame(results)

    # Merge back to original DataFrame
    df = df.merge(pdc_df, on=['person_id', 'drug_name'], how='left')

    return df
