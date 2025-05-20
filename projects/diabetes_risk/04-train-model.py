import marimo

__generated_with = "0.13.4"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import pandas as pd
    import matplotlib.pyplot as plt
    import xgboost as xgb
    import numpy as np
    import shap
    from sklearn.model_selection import train_test_split
    from sklearn.metrics import mean_squared_error, r2_score, accuracy_score, confusion_matrix, ConfusionMatrixDisplay
    from dotenv import load_dotenv
    from phmlondon.snow_utils import SnowflakeConnection
    from phmlondon.feature_store_manager import FeatureStoreManager
    return (
        ConfusionMatrixDisplay,
        SnowflakeConnection,
        accuracy_score,
        confusion_matrix,
        load_dotenv,
        mean_squared_error,
        np,
        plt,
        r2_score,
        shap,
        train_test_split,
        xgb,
    )


@app.cell
def _(SnowflakeConnection, load_dotenv):
    load_dotenv('.env') # very weirdly, marimo changes the default behaviour of load_dotenv to look next to pyproject.toml first rather than in the cwd, so need to specify - https://docs.marimo.io/guides/configuration/runtime_configuration/#env-files
    conn = SnowflakeConnection()
    return (conn,)


@app.cell
def _(conn):
    query = "SELECT * FROM INTELLIGENCE_DEV.AI_CENTRE_FEATURE_STORE.HBA1C_MODEL_ALL_FEATURES_V1;"

    df = conn.session.sql(query).to_pandas()
    return (df,)


@app.cell
def _(df):
    df.head()
    return


@app.cell
def _(df):
    df_pruned = df.drop(columns=[
        'PERSON_ID', 'GENDER_CONCEPT_ID', 'DATE_OF_BIRTH', 'DATE_OF_DEATH',
        'DATE_OF_DEATH_INC_CODES', 'CURRENT_ADDRESS_ID', 'ETHNIC_CODE_CONCEPT_ID',
        'APPROX_CURRENT_AGE', 'LATEST_DIAGNOSIS_DATE', 'EARLIEST_DIAGNOSIS_DATE',
        'CORE_CONCEPT_ID', 'EARLIEST_HBA1C_DIAGNOSIS_DATE', 'EARLIEST_CODE_DIAGNOSIS_DATE',
        'ETHNIC_AIC_CATEGORY', 'SMOKING_STATUS', 'LATEST_SMOKING_STATUS_DATE',
        'EARLIEST_DIAGNOSIS_DATE_COMBINED', 'OUTCOME_DATE', 'START_OF_BLINDED_PERIOD',
        'IMD_DECILE', 'IMD_QUINTILE'
    ])

    return (df_pruned,)


@app.cell
def _(df_pruned):
    df_pruned.head()
    return


@app.cell
def _(df_pruned, plt):
    # Plot outcome HbA1c distribution

    plt.figure(figsize=(8, 5))
    plt.hist(df_pruned['OUTCOME_HBA1C'], bins=30, color='skyblue', edgecolor='black')
    plt.title('Distribution of OUTCOME_HBA1C')
    plt.xlabel('HbA1c (mmol/mol)')
    plt.ylabel('Frequency')
    plt.grid(True, linestyle='--', alpha=0.6)
    plt.tight_layout()
    plt.show()

    return


@app.cell
def _(df_pruned, plt):
    # Plot slopes

    plt.figure(figsize=(8, 5))
    plt.hist(df_pruned['SLOPE_OF_HBA1C_OVER_TIME'], bins=30, color='skyblue', edgecolor='black')
    plt.title('Distribution of HbA1c slope')
    plt.xlabel('HbA1c change (mmol/mol/month)')
    plt.ylabel('Frequency')
    plt.grid(True, linestyle='--', alpha=0.6)
    plt.xlim(-3, 3)
    plt.tight_layout()
    plt.show()
    return


@app.cell
def _(df_pruned):
    df_pruned['SLOPE_OF_HBA1C_OVER_TIME']
    return


@app.cell
def _(df_pruned):
    X = df_pruned.drop(columns=['OUTCOME_HBA1C'])
    y = df_pruned['OUTCOME_HBA1C']
    return X, y


@app.cell
def _(X, train_test_split, y):
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    return X_test, X_train, y_test, y_train


@app.cell
def _(xgb):
    model = xgb.XGBRegressor(
        objective='reg:squarederror',
        n_estimators=100,
        max_depth=6,
        learning_rate=0.1,
        random_state=42
    )
    return (model,)


@app.cell
def _(X_train, model, y_train):
    model.fit(X_train, y_train)
    return


@app.cell
def _(X_test, model):
    y_pred = model.predict(X_test)
    return (y_pred,)


@app.cell
def _(y_pred):
    print(y_pred)
    return


@app.cell
def _(mean_squared_error, np, plt, r2_score, y_pred, y_test):
    mse = mean_squared_error(y_test, y_pred)
    rmse = np.sqrt(mse)
    r2 = r2_score(y_test, y_pred)
    print(f"Mean Squared Error: {mse}")
    print(f"Root Mean Squared Error: {rmse}")
    print(f"R^2: {r2}")

    plt.figure()
    plt.scatter(y_test, y_pred, s=2, color='lightcoral')
    plt.xlabel("True HbA1c (mmol/mol)")
    plt.ylabel("Predicted HbA1c (mmol/mol)")
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
    plt.text(lims[0] + 5, lims[1] - 5, f"R²: {r2:.2f}\nRMSE: {rmse:.2f}", fontsize=10)
    plt.title("Model performance")
    return


@app.cell
def _(
    ConfusionMatrixDisplay,
    accuracy_score,
    confusion_matrix,
    np,
    plt,
    y_pred,
    y_test,
):
    bin_edges = np.arange(0, np.max([y_test.max(), y_pred.max()]) + 10, 10)
    bin_labels = [f"{int(left)}–{int(right)}" for left, right in zip(bin_edges[:-1], bin_edges[1:])]
    y_test_binned = np.digitize(y_test, bins=bin_edges, right=False) - 1
    y_pred_binned = np.digitize(y_pred, bins=bin_edges, right=False) - 1
    n_bins = len(bin_labels)
    y_test_binned = np.clip(y_test_binned, 0, n_bins - 1)
    y_pred_binned = np.clip(y_pred_binned, 0, n_bins - 1)

    accuracy = accuracy_score(y_test_binned, y_pred_binned)
    print(f"Classification accuracy (binned): {accuracy:.2f}")

    cm = confusion_matrix(y_test_binned, y_pred_binned, labels=range(n_bins))
    disp = ConfusionMatrixDisplay(confusion_matrix=cm, display_labels=bin_labels)

    fig, _ax = plt.subplots(figsize=(10, 8))
    disp.plot(ax=_ax, cmap='Reds', xticks_rotation=45)
    plt.title("Confusion Matrix of Binned HbA1c Predictions")
    plt.tight_layout()
    plt.show()

    return


@app.cell
def _(model, plt, xgb):
    importances = model.feature_importances_

    xgb.plot_importance(model, importance_type='gain', max_num_features=30)
    plt.title("Top 30 Feature Importances")
    plt.show()
    return


@app.cell
def _(X_test, model, shap):
    explainer = shap.Explainer(model)
    shap_values = explainer(X_test)
    shap.summary_plot(shap_values, X_test)
    shap.force_plot(explainer.expected_value, shap_values[0], X_test.iloc[0])

    return


if __name__ == "__main__":
    app.run()
