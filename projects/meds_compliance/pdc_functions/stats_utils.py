import numpy as np
import statsmodels.api as sm
import statsmodels.formula.api as smf
import pandas as pd
import warnings
from statsmodels.tools.sm_exceptions import PerfectSeparationError


def unadjusted_logr(df, outcome_col='outcome_binary', predictor_col='pre_exclusive_pdc'):
    """
    Runs an unadjusted logistic regression with a binary outcome and continuous predictor.

    Parameters:
    - df: pandas DataFrame
    - outcome_col: str, name of binary outcome column ('good' = 1, 'poor' = 0)
    - predictor_col: str, name of continuous predictor column

    Returns:
    - model: fitted statsmodels logistic regression model
    - odds_ratios: DataFrame with ORs and 95% CI
    """
    # Copy dataframe to avoid modifying original
    df = df.copy()

    # Fit the model
    model = smf.logit(f'{outcome_col} ~ {predictor_col}', data=df).fit(disp=False)

    return model



def adjusted_logr(df, outcome_col='outcome_binary',
                  predictor_col='pre_exclusive_pdc', covariates=None):
    """
    Runs an adjusted logistic regression with a binary outcome
    and continuous predictor plus additional covariates.
    """

    df = df.copy()

    # Build formula
    formula = f'{outcome_col} ~ {predictor_col}'
    if covariates:
        # Wrap categorical covariates in C()
            encoded_covariates = [
                f"C({col})" if df[col].dtype == 'object' or df[col].nunique() < 12 else col
                for col in covariates
            ]
            formula += ' + ' + ' + '.join(encoded_covariates)

    try:
        model = smf.logit(formula, data=df).fit(disp=False)
    except np.linalg.LinAlgError:
        raise np.linalg.LinAlgError(f"Singular matrix error while fitting model with formula: {formula}")
    except PerfectSeparationError:
        raise PerfectSeparationError("Perfect separation detected, logistic regression cannot be fit.")


    return model



def multilevel_unadjusted_logr(df, outcome_col='outcome_binary',
                               predictor_col='pre_exclusive_pdc', cluster_col='person_id'):
    """
    Runs a hierarchical logistic regression using GEE with clustering on person_id.

    Parameters:
    - df: pandas DataFrame
    - outcome_col: str, name of binary outcome column ('good' = 1, 'poor' = 0)
    - predictor_col: str, name of continuous predictor column
    - cluster_col: str, ID for clustering (e.g., person ID)

    Returns:
    - model: fitted statsmodels GEE model
    - odds_ratios: DataFrame with ORs and 95% CI
    """
    df = df.copy()

    # Fit GEE logistic model with clustering
    model = smf.gee(f'{outcome_col} ~ {predictor_col}',
                    groups=cluster_col,
                    data=df,
                    family=sm.families.Binomial()).fit()

    return model

def multilevel_adjusted_logr(df, outcome_col='outcome_binary', predictor_col='dynamic_pdc',
                              covariates=None, cluster_col='person_id'):
    """
    Runs a multilevel logistic regression using GEE
    with clustering on one or more groups (e.g., person_id).

    Parameters:
    - df: pandas DataFrame
    - outcome_col: str, name of binary outcome column ('good' = 1, 'poor' = 0)
    - predictor_col: str, main predictor (e.g., continuous)
    - covariates: list of str, other covariates to adjust for (optional)
    - cluster_col: str, column name used for clustering (e.g., person_id)

    Returns:
    - model: fitted statsmodels GEE model
    - odds_ratios: DataFrame with ORs and 95% CI
    """
    df = df.copy()

    # Build the formula

    formula = f'{outcome_col} ~ {predictor_col}'
    if covariates:
        # Wrap categorical covariates in C()
            encoded_covariates = [
                f"C({col})" if df[col].dtype == 'object' or df[col].nunique() < 12 else col
                for col in covariates
            ]
            formula += ' + ' + ' + '.join(encoded_covariates)

    # Fit GEE logistic model with clustering
    model = smf.gee(formula=formula,
                    groups=cluster_col,
                    data=df,
                    family=sm.families.Binomial()).fit()

    return model



def save_results_to_csv(results, model_name):
    """
    Saves the results to a CSV file based on the model type.
    """
    filename = f"{model_name}_results.csv"
    results.to_csv(filename, index=False)
    print(f"Results saved to {filename}")

def fit_and_save_models_for_pdc(df, outcome_col='medication_compliance', pdc_cols=None, covariates=None, cluster_col='person_id'):
    """
    Fits unadjusted, adjusted, and multilevel models for each combination of PDC variables (inclusive/exclusive, pre/post),
    and saves the results in separate files, including p-values, 95% confidence intervals, model coefficients, and other covariates.
    """
    if pdc_cols is None:
        pdc_cols = ['inclusive_pdc', 'exclusive_pdc', 'pre_inclusive_pdc', 'pre_exclusive_pdc', 'post_inclusive_pdc', 'post_exclusive_pdc']

    # Function to compute odds ratios and 95% confidence intervals
    def compute_odds_ratios(model):
        odds_ratios = pd.DataFrame({
            'covariate': model.params.index,
            'coefficient': model.params.values,
            'p_value': model.pvalues.values,
            'OR': model.params.apply(lambda x: np.exp(x)),  # Exponentiate to get OR
            'CI_lower': np.exp(model.conf_int()[0]),  # Lower bound of the 95% CI
            'CI_upper': np.exp(model.conf_int()[1]),  # Upper bound of the 95% CI
        })
        return odds_ratios

    models_dict = {}

    # Unadjusted Logistic Regression for each PDC column
    unadjusted_results = []
    for pdc_col in pdc_cols:

        model = unadjusted_logr(df, outcome_col=outcome_col, predictor_col=pdc_col)
        models_dict[f'unadjusted_{pdc_col}'] = model
        model_result = compute_odds_ratios(model)
        model_result['pdc_col'] = pdc_col
        model_result['model'] = 'unadjusted'
        model_result['covariates'] = None
        unadjusted_results.append(model_result)

    unadjusted_results_df = pd.concat(unadjusted_results)
    save_results_to_csv(unadjusted_results_df, model_name="unadjusted")

    # Adjusted Logistic Regression for each PDC column
    adjusted_results = []
    for pdc_col in pdc_cols:
        base_covariates = covariates or []

        exposure_covariate = []
        if 'pre' in pdc_col:
            exposure_covariate = ['total_pre_exposure_days']
        elif 'post' in pdc_col:
            exposure_covariate = ['total_post_exposure_days']

        selected_covariates = base_covariates + exposure_covariate

        model = adjusted_logr(df, outcome_col=outcome_col, predictor_col=pdc_col, covariates=selected_covariates)
        models_dict[f'adjusted_{pdc_col}'] = model
        model_result = compute_odds_ratios(model)
        model_result['pdc_col'] = pdc_col
        model_result['model'] = 'adjusted'
        model_result['covariates'] = ', '.join(selected_covariates)
        adjusted_results.append(model_result)

    adjusted_results_df = pd.concat(adjusted_results)
    save_results_to_csv(adjusted_results_df, model_name="adjusted")

    # Multilevel Logistic Regression (Unadjusted) for each PDC column
    multilevel_unadjusted_results = []
    for pdc_col in pdc_cols:

        model = multilevel_unadjusted_logr(df, outcome_col=outcome_col, predictor_col=pdc_col, cluster_col=cluster_col)
        models_dict[f'multilevel_unadjusted_{pdc_col}'] = model
        model_result = compute_odds_ratios(model)
        model_result['pdc_col'] = pdc_col
        model_result['model'] = 'multilevel_unadjusted'
        model_result['covariates'] = None
        multilevel_unadjusted_results.append(model_result)

    multilevel_unadjusted_results_df = pd.concat(multilevel_unadjusted_results)
    save_results_to_csv(multilevel_unadjusted_results_df, model_name="multilevel_unadjusted")

    # Multilevel Logistic Regression (Adjusted) for each PDC column
    multilevel_adjusted_results = []
    for pdc_col in pdc_cols:
        base_covariates = covariates or []

        exposure_covariate = []
        if 'pre' in pdc_col:
            exposure_covariate = ['total_pre_exposure_days']
        elif 'post' in pdc_col:
            exposure_covariate = ['total_post_exposure_days']

        selected_covariates = base_covariates + exposure_covariate

        model = multilevel_adjusted_logr(df, outcome_col=outcome_col, predictor_col=pdc_col, covariates=selected_covariates, cluster_col=cluster_col)
        models_dict[f'multilevel_adjusted_{pdc_col}'] = model
        model_result = compute_odds_ratios(model)
        model_result['pdc_col'] = pdc_col
        model_result['model'] = 'multilevel_adjusted'
        model_result['covariates'] = ', '.join(selected_covariates)
        multilevel_adjusted_results.append(model_result)

    multilevel_adjusted_results_df = pd.concat(multilevel_adjusted_results)
    save_results_to_csv(multilevel_adjusted_results_df, model_name="multilevel_adjusted")

    return models_dict


def linear_reg(df, predictor_col, outcome_col):
    """
    Performs an unadjusted linear regression of an outcome on a predictor.

    Parameters:
        df (pd.DataFrame): The input dataframe.
        predictor_col (str): Name of the predictor column.
        outcome_col (str): Name of the outcome column.

    Returns:
        RegressionResults: The fitted OLS model result.
    """

    # Define predictor and outcome
    X = df[predictor_col]
    y = df[outcome_col]

    # Add constant for intercept
    X = sm.add_constant(X)

    # Fit the model
    model = sm.OLS(y, X).fit()

    return model

def prep_for_regression(df, ethnicity_col='ethnicity', imd_col='imd', gender_col='gender', 
                        outcome_col='medication_compliance', predictor_col="pre_exclusive_pdc", 
                        covariates=None, cluster_col='person_id', threshold=5000):

    """
    Clean dataframe for regression:
    - Convert outcome to binary
    - Drop NA for relevant columns
    - Filter out unwanted gender and IMD categories
    - Group rare ethnicity categories into 'Other'

    Parameters:
        df (pd.DataFrame): Input dataframe.
        ethnicity_col (str): Ethnicity column name.
        imd_col (str): IMD column name.
        gender_col (str): Gender column name.
        outcome_col (str): Outcome column name.
        predictor_col (str): Predictor column name.
        covariates (list): List of covariate column names.
        cluster_col (str): Cluster/grouping column name.
        threshold (int): Minimum count threshold for ethnicity grouping.

    Returns:
        pd.DataFrame: Cleaned dataframe ready for regression.
    """
    df = df.copy()

    # Convert outcome to binary
    df['outcome_binary'] = df[outcome_col].map({'good': 1, 'poor': 0})

    # Columns to keep
    if isinstance(predictor_col, str):
            predictor_col = [predictor_col]

        # Combine all required columns
    cols_to_check = predictor_col + ['outcome_binary', cluster_col]
    if covariates:
        cols_to_check += covariates

    # Drop rows with NA in these columns
    df = df[cols_to_check].dropna()

    # Filter out unwanted gender categories
    df = df[~df[gender_col].isin(["Unknown", "Other", "None"])]

    # Handle IMD filtering/removal of 'nan' category
    if pd.api.types.is_categorical_dtype(df[imd_col]):
        # Remove 'nan' category safely
        if 'nan' in df[imd_col].cat.categories:
            df[imd_col] = df[imd_col].cat.remove_categories(['nan'])
    else:
        # Filter out string 'nan' entries if present
        df = df[df[imd_col] != 'nan']

    # Recode ethnicity rare categories
    counts = df[ethnicity_col].value_counts()
    rare_cats = counts[counts < threshold].index
    df[ethnicity_col] = df[ethnicity_col].apply(lambda x: 'Other' if x in rare_cats else x)

    return df
