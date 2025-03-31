import random
from datetime import datetime, timedelta

import pandas as pd


# Function to generate dummy medication orders data
def generate_medication_data(num_people=50, num_entries_per_person=50, gap_probability=0.1):
    # List to store the data
    data = []

    # Generate data for each person
    for person_id in range(1, num_people + 1):
        # Random start date from 2020 to 2025
        start_date = datetime(2020, 1, 1)  # Ensure start date is in 2020 or later
        last_order_date = start_date + timedelta(days=random.randint(0, 365 * 5))  # First order date

        # Generate orders for each person with realistic intervals
        for _ in range(num_entries_per_person):
            # Drug name (all atorvastatin for dummy data)
            drug = "atorvastatin"

            # Random duration in days (between 7 and 90 days)
            duration_days = random.randint(7, 90)

            # Dose instruction (take once at night)
            dose = "Take once at night"

            # Randomly decide if a gap occurs (non-compliance)
            gap_occurred = random.random() < gap_probability  # True with a probability of `gap_probability`

            if gap_occurred:
                # Add a large gap (e.g., 60 to 180 days) to simulate missing a medication refill
                gap_days = random.randint(60, 180)
                last_order_date = last_order_date + timedelta(days=gap_days)  # Add the gap

            # Define quantity and quantity unit
            quantity = duration_days  # Assuming 1 pill per day for simplicity
            quantity_unit = "pills"

            # Append the current order data
            order_date = last_order_date
            data.append([person_id, drug, duration_days, order_date, dose, quantity, quantity_unit])

            # Calculate the interval for the next order (between 7 and 60 days)
            next_order_interval = random.randint(7, 60)  # Days between orders
            last_order_date = order_date + timedelta(days=next_order_interval)

    # Create DataFrame
    df = pd.DataFrame(data, columns=['person_id', 'drug', 'duration_days', 'order_date', 'dose', 'quantity', 'quantity_unit'])

    return df
