import pandas as pd


def define_treatment_periods(df, gap_threshold=None):
    """
    Assigns period IDs based on treatment gaps.

    Arguments:
        df : DataFrame containing prescription orders.
        gap_threshold : Integer defining treatment gaps
        (default: uses order-specific duration_days).

    Returns:
        df : Updated DataFrame with 'period_id'.
    """

    date_cols = ["order_date", "order_enddate"]
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    # Ensure gap_threshold is numeric
    if gap_threshold is not None:
        df["gap_value"] = gap_threshold
    else:
        # Fill missing 'calculated_duration' with 'duration_days' and convert to numeric
        filled_duration = df["calculated_duration"].fillna(df["duration_days"])
        df["gap_value"] = pd.to_numeric(filled_duration, errors="coerce")

    # Sort & rank orders per Person & Drug
    df = df.sort_values(["person_id", "drug", "order_date"])
    df["order_rank"] = df.groupby(["person_id", "drug"]).cumcount() + 1

    # Compute Days to Next Order
    df["next_order_date"] = df.groupby(["person_id", "drug"])["order_date"].shift(-1)
    df["days_to_next_order"] = (df["next_order_date"] - df["order_enddate"]).dt.days
    df["days_to_next_order"] = df["days_to_next_order"].fillna(0).clip(lower=0)

    # Identify New Treatment Periods
    df["new_period_flag"] = (df["days_to_next_order"] > df["gap_value"]).astype(int)
    df["shifted_flag"] = df.groupby(["person_id", "drug"])["new_period_flag"].shift(1, fill_value=0)
    df["period_id"] = df.groupby(["person_id", "drug"])["shifted_flag"].cumsum() + 1

    # Exclude Last Gap in a Period (Represents Treatment Break)
    df["days_to_next_order"] = df["days_to_next_order"].where(df["new_period_flag"] != 1, 0)

    return df


def period_pdc(df):
    """
    Summarises treatment periods per person and drug and calculates PDC.

    Arguments:
        df : DataFrame with period IDs assigned.

    Returns:
        period_summary : DataFrame summarising each period including a PDC calculation.
    """

    # Grouping and aggregating data
    period_summary = df.groupby(["person_id", "drug", "period_id"]).agg(
        start_date=("order_date", "min"),
        end_date=("order_enddate", "max"),
        order_gaps=("days_to_next_order", "sum"),
    ).reset_index()

    # Calculate duration of the period
    period_summary["duration_period"] = (
        period_summary["end_date"] - period_summary["start_date"]
    ).dt.days

    # Calculate duration of the orders
    duration_orders = period_summary["duration_period"] - period_summary["order_gaps"]
    period_summary["duration_orders"] = duration_orders

    # Calculate Estimated PDC
    est_pdc = duration_orders / period_summary["duration_period"]
    period_summary["est_pdc"] = est_pdc

    # Avoid division by zero: return NaN instead of None
    period_summary["est_pdc"] = period_summary["est_pdc"].where(
        period_summary["duration_period"] > 0, float("nan")
    )

    return period_summary
