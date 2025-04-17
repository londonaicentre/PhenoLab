import re

import pandas as pd
from word2number import w2n

def process_dose(dose):
        """Clean the dose column and extract daily tablet count.
        Args:
            dose: A single dose entry (string).
        Returns:
            Calculated tablets per day or None.
        """
        if not isinstance(dose, str):  # Handle cases where dose is NaN or not a string
            return None

        dose = dose.lower().strip()

        # Frequency mapping dictionary
        frequency_mapping = {
            "once": 1,
            "twice": 2,
            "three times": 3,
            "four times": 4,
            "daily": 1,
            "per day": 1,
            "every morning": 1,
            "at night": 1,
            "every evening": 1,
            "morning": 1,
            "night": 1,
            "once a day": 1,
            "twice a day": 2,
            "three times a day": 3,
            "four times a day": 4,
        }

        # Extract frequency first
        frequency_match = None
        for phrase, frequency in frequency_mapping.items():
            if phrase in dose:
                frequency_match = frequency
                break

        if not frequency_match:
            frequency_match = 1  # Default to once daily if no specific frequency found

       # Extract tablet count (defaults to 1 if not found)
        tablet_count = 1  # Default to 1
        tablet_match = re.search(r'(\d+|one|two|three|four|five|six|seven|eight|nine|ten)', dose)
        if tablet_match:
            tablet_count = tablet_match.group(1)
            if tablet_count.isdigit():
                tablet_count = int(tablet_count)
            else:
                tablet_count = w2n.word_to_num(tablet_count)

        # Return daily tablets per day
        return tablet_count * frequency_match if frequency_match else None


def clean_dose(df):
    """Cleans the 'dose' column in a given DataFrame.
    Args:
        df: pandas DataFrame with columns called 'dose' and 'quantity'.
    Returns:
        DataFrame with an additional column 'tablets_per_day' and 'calculated_duration'
        and 'order_enddate'
    """


    # Apply function to "dose" column
    df["tablets_per_day"] = df["dose"].apply(process_dose)

    # Now calculate the "calculated_duration" based on the quantity and tablets_per_day
    df["calculated_duration"] = df.apply(
        lambda row: float(row["quantity_value"]) / row["tablets_per_day"] if (
            row["tablets_per_day"] and row["quantity_unit"] and
            re.search(r"tablet(s)?|capsule(s)?", row["quantity_unit"], re.IGNORECASE)
        ) else None, axis=1
    )

    ## Fill missing values in 'calculated_duration' with 'duration_days'
    duration = df["calculated_duration"].fillna(df["duration_days"])

    # Convert 'duration' to a timedelta and add it to 'order_date'
    df["order_enddate"] = df["order_date"] + pd.to_timedelta(duration, unit='D')
    return df  # Return modified DataFrame


def covered_days(df):
    """
    Determines the 'covered_days' value for each row in the DataFrame 
    based on 'calculated_duration' and 'duration_days'.

    Logic:
    - If both values are equal and not null, return either.
    - If they differ:
        - Prefer 'calculated_duration' if it's not null and greater than zero.
        - Otherwise, use 'duration_days' if it's not null.

    Args:
        df (DataFrame): A DataFrame containing 'calculated_duration' and 'duration_days' columns.

    Returns:
        Series: A Series containing the 'covered_days' values for each row.
    """

    return df.apply(
        lambda row: row["calculated_duration"]
        if pd.notnull(row["calculated_duration"]) and pd.notnull(row["duration_days"])
        and row["calculated_duration"] == row["duration_days"]
                  else (row["calculated_duration"]
                        if pd.notnull(row["calculated_duration"]) and row["calculated_duration"] > 0
                        else row["duration_days"] if pd.notnull(row["duration_days"])
                        else None), axis=1)
