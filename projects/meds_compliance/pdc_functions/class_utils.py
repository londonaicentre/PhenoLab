import re
from datetime import datetime

import numpy as np
import pandas as pd
from word2number import w2n


class MedicationTable:
    def __init__(self, df):
        self.df = df

    REQUIRED_COLUMNS = [
        "person_id",  "drug_name",
        "dose", "quantity_value", "quantity_unit",  "order_date", "duration_days"
    ]

    COLUMN_TYPES = {
        "person_id": int,
        "drug_name": str,
        "dose": str,
        "quantity_value": float,
        "quantity_unit": str,
        "duration_days": int,
        "days_to_next_order": int

    }

    DATE_COLUMNS = ["order_date"]
    DATE_FORMAT = "%Y-%m-%d"

    def normalise_missing_values(self):
        missing_values = ['none', 'NA', 'na', 'null', '', None, 'None', 'NaN', pd.NaT, np.nan, '<NA>']
        for col in self.df.columns:
            self.df[col] = self.df[col].replace(missing_values, pd.NA)
        return self

    def convert_dates(self):
        for col in MedicationTable.DATE_COLUMNS:
            if col in self.df.columns:
                self.df[col] = pd.to_datetime(self.df[col], format=MedicationTable.DATE_FORMAT, errors='coerce')
        return self

    def validate_columns(self):
        missing = [col for col in MedicationTable.REQUIRED_COLUMNS if col not in self.df.columns]
        if missing:
            raise ValueError(f"Missing required columns: {missing}")

    def validate_data_types(self):
        for col, expected_type in MedicationTable.COLUMN_TYPES.items():
            if col in self.df.columns:
                if not pd.api.types.is_dtype_equal(self.df[col].dtype, expected_type):
                    try:
                        if expected_type == int:
                            self.df[col] = self.df[col].astype('Int64')
                        elif expected_type == float:
                            self.df[col] = self.df[col].astype('Float64')  # Nullable float type
                        else:
                            self.df[col] = self.df[col].astype(expected_type)
                    except Exception as err:
                        raise TypeError(f"Column '{col}' must be of type {expected_type}. Attempted conversion failed: {err}") from err

    @staticmethod
    def process_dose(dose):
        if not isinstance(dose, str):
            return None
        dose = dose.lower().strip()
        dose = dose.replace('0','o')
        frequency_mapping = {
            "once": 1, "twice": 2, "three times": 3, "four times": 4,
            "daily": 1, "per day": 1, "every morning": 1, "at night": 1,
            "every evening": 1, "morning": 1, "night": 1, "once a day": 1,
            "twice a day": 2, "three times a day": 3, "four times a day": 4,
            "od":1, "bd":2, "tds":3, "qds":4, 'o d':1
        }

        frequency = next((v for k, v in frequency_mapping.items() if k in dose), 1)

        tablet_match = re.search(r'(\d+(\.\d+)?|one|two|three|four|five|six|seven|eight|nine|ten|half)', dose)
        if tablet_match:
            count = tablet_match.group(1)
            # Try converting word to number
            try:
                if count.isdigit():
                    tablet_count = int(count)
                else:
                    tablet_count = w2n.word_to_num(count)
            except ValueError:
                # If w2n fails, handle it here - for example, default to 1
                tablet_count = 1
        else:
            tablet_count = 1  # Default to 1 if no match

        return tablet_count * frequency

    def clean_dose(self):
        self.df["tablets_per_day"] = self.df["dose"].apply(MedicationTable.process_dose)

        # Check for rows where tablets_per_day is 0 or not a valid number
        invalid_rows = self.df[~self.df['tablets_per_day'].apply(lambda x: isinstance(x, (int, float)) and x > 0)]
        print("Invalid rows (dose or tablets_per_day issue):")
        print(invalid_rows[["dose", "tablets_per_day"]])

        self.df["calculated_duration"] = self.df.apply(
            lambda row: float(row["quantity_value"]) / row["tablets_per_day"]
            if pd.notnull(row["tablets_per_day"]) and pd.notnull(row["quantity_value"]) and
               pd.notnull(row["quantity_unit"]) and re.search(r"tablet(s)?|capsule(s)?", str(row["quantity_unit"]), re.IGNORECASE)
            else None, axis=1
        )

        duration = self.df["calculated_duration"].fillna(self.df["duration_days"])
        self.df["order_enddate"] = self.df["order_date"] + pd.to_timedelta(duration, unit='D')

        return self

    def calculate_covered_days(self):
        self.df["covered_days"] = self.df.apply(
            lambda row: row["calculated_duration"]
            if pd.notnull(row["calculated_duration"]) and pd.notnull(row["duration_days"]) and
               row["calculated_duration"] == row["duration_days"]
            else (
                row["calculated_duration"]
                if pd.notnull(row["calculated_duration"]) and row["calculated_duration"] > 0
                else (
                    row["duration_days"]
                    if pd.notnull(row["duration_days"]) and row["duration_days"] > 0
                else None
                )
            ), axis=1
        )
        return self
