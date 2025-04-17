from datetime import datetime

import numpy as np
import pandas as pd


class MedicationTable:
    def __init__(self, df):
        self.df = df
    # Define the required columns and their expected types
    REQUIRED_COLUMNS = [
        "order_id", "person_id", "medication_statement_id", "drug_name",
        "dose", "quantity_value", "quantity_unit", "drug_description",
        "class", "core_concept_id", "order_date", "duration_days",
        "days_to_next_order", "statement_date", "statement_enddate",
        "medication_compliance"
    ]

    COLUMN_TYPES = {
        "order_id": int,
        "person_id": int,
        "medication_statement_id": int,
        "drug_name": str,
        "dose": str,
        "quantity_value": float,
        "quantity_unit": str,
        "drug_description": str,
        "class": str,
        "core_concept_id": int,
        "duration_days": int,
        "days_to_next_order": int,
        "medication_compliance": str
    }

    DATE_COLUMNS = ["order_date", "statement_date", "statement_enddate"]
    DATE_FORMAT = "%Y-%m-%d"


    def normalise_missing_values(self):
        """
        Standardises various forms of missing values to NaN.
        """
        missing_values = ['none', 'NA', 'na', 'null', '', None, 'None', 'NaN', pd.NaT, np.nan, '<NA>']
        for col in self.df.columns:
            self.df[col] = self.df[col].replace(missing_values, pd.NA)
        return self


    def convert_dates(self):
        """
        Converts all date columns to datetime, coercing errors to NaT.
        """
        for col in MedicationTable.DATE_COLUMNS:
            if col in self.df.columns:
                self.df[col] = pd.to_datetime(self.df[col], format=MedicationTable.DATE_FORMAT, errors='coerce')
        return self


    def validate_columns(self):
        """
        Validates if all required columns are present in the DataFrame.
        """
        missing = [col for col in MedicationTable.REQUIRED_COLUMNS if col not in self.df.columns]
        if missing:
            raise ValueError(f"Missing required columns: {missing}")


    def validate_data_types(self):
        """
        Validates the data types for each column in the DataFrame.
        Attempts to convert the column to the expected type if mismatched.
        Raises TypeError if conversion fails.
        """
        for col, expected_type in MedicationTable.COLUMN_TYPES.items():
            if col in self.df.columns:
                if not pd.api.types.is_dtype_equal(self.df[col].dtype, expected_type):
                    try:
                        if expected_type == int:
                            # Use pandas nullable Int64 type to allow NaNs
                            self.df[col] = self.df[col].astype('Int64')
                        else: self.df[col] = self.df[col].astype(expected_type)
                    except Exception as err:
                        raise TypeError( f"Column '{col}' must be of type {expected_type}. "f"Attempted conversion failed: {err}") from err