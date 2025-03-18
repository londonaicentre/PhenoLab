from dotenv import load_dotenv
from pathlib import Path

from phmlondon.snow_utils import SnowflakeConnection
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, r2_score
from xgboost import XGBRegressor
import matplotlib.pyplot as plt
from matplotlib.patches import Polygon
import numpy as np

import pandas as pd

def create_tables(conn: SnowflakeConnection):
    for filename in [f for f in Path('create_tables').iterdir() if f.is_file()]:
        with open(filename) as fid:
            sql = fid.read()
            conn.session.sql(sql).collect()
    
    print('SQL scripts in create_tables directory executed')

def load_and_process_data(conn: SnowflakeConnection) -> tuple[pd.DataFrame, pd.DataFrame, pd.Series, pd.Series]:
    with open('join_tables.sql') as fid:
        sql = fid.read()
        conn.session.sql(sql).collect()
    
    patients = conn.session.sql("""
    select *
    from INTELLIGENCE_DEV.AI_CENTRE_FEATURE_STORE.T2DM_PATIENTS_WITH_HBA1C_BINS
    where mod(abs(hash(person_id)), 100) < 10
    """).to_pandas()

    print('Data of size', patients.shape, 'loaded')

    print('Data:')
    print(patients.head())

    X = patients.drop(["PERSON_ID", "DOB", "DOD", "DOD_INC_CODES", 
                   "APPROX_CURRENT_AGE", "FINAL_HBA1C"], axis=1)
    y = patients["FINAL_HBA1C"]
    patients["GENDER_CONCEPT_ID"].astype("category") #this is needed to make XGboost understand these columns are categorical
    patients["ETHNIC_CODE_CONCEPT_ID"].astype("category") 
    print('Data after dropping columns:')
    print(X.head())

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    print('Data split into train and test sets. X train has size', X_train.shape, 'and X test has size', X_test.shape)

    plot_sample_of_data(patients)

    return X_train, X_test, y_train, y_test

def model_fit_and_evaluate(X_train: pd.DataFrame, X_test: pd.DataFrame, y_train: pd.Series, y_test: pd.Series):
    model = XGBRegressor()
    model.fit(X_train, y_train)
    y_pred = model.predict(X_test)
    mse = mean_squared_error(y_test, y_pred)
    r2 = r2_score(y_test, y_pred)
    print(f"Mean Squared Error: {mse}")
    print(f"R^2: {r2}")

    plt.scatter(y_pred, y_test, s=2, color='lightcoral')
    plt.xlabel("Predicted HbA1c (mmol/mol)")
    plt.ylabel("True HbA1c (mmol/mol)")
    ax = plt.gca()
    lims = [
        np.min([ax.get_xlim(), ax.get_ylim()]),  # min of both axes
        np.max([ax.get_xlim(), ax.get_ylim()]),  # max of both axes
    ]
    # now plot both limits against eachother
    ax.plot(lims, lims, 'k-', alpha=0.75, zorder=0)
    ax.set_aspect('equal')
    ax.set_xlim(lims)
    ax.set_ylim(lims)
    plt.text(100, 20, f"R^2: {r2:.2f}")
    plt.show()

def plot_sample_of_data(patients: pd.DataFrame):
    plt.figure()
    i = 0
    for i, row in patients[['HBA1C_10_YEARS_AGO', 
                                    'HBA1C_9_YEARS_AGO', 
                                    'HBA1C_8_YEARS_AGO', 
                                    'HBA1C_7_YEARS_AGO', 
                                    'HBA1C_6_YEARS_AGO', 
                                    'FINAL_HBA1C']].iterrows():
        plt.plot([-10, -9, -8, -7, -6, 0], row, color='salmon')
        i += 1
        if i > 100:
            break
    plt.xlabel('')
    ax = plt.gca()
    ylim = ax.get_ylim()[1]
    poly = Polygon([[-6, 0], [0, 0], [0, ylim], [-6, ylim]], closed=True, fill=True, color='lightgrey', alpha=0.25)
    ax.add_patch(poly)
    ax.set_xlim([-10, 0])
    plt.xlabel('Years prior to final HbA1c')
    plt.ylabel('HbA1c (mmol/mol)')

if __name__ == "__main__":
    load_dotenv()
    conn = SnowflakeConnection()
    # create_tables(conn)
    X_train, X_test, y_train, y_test = load_and_process_data(conn)
    model_fit_and_evaluate(X_train, X_test, y_train, y_test)
    


