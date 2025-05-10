import numpy as np
import statsmodels.api as sm
import statsmodels.formula.api as smf
import pandas as pd


def unadjusted_logr(df, outcome_col='medication_compliance', predictor_col='dynamic_pdc'):
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

    # Convert outcome to binary (assumes 'good' is positive class)
    df['outcome_binary'] = df[outcome_col].map({'good': 1, 'poor': 0})

    # Drop missing values in required columns
    df = df[[predictor_col, 'outcome_binary']].dropna()

    # Fit the model
    model = smf.logit(f'outcome_binary ~ {predictor_col}', data=df).fit(disp=False)

    # Calculate odds ratios with 95% CI
    params = model.params
    conf = model.conf_int()
    conf['OR'] = params
    conf.columns = ['2.5%', '97.5%', 'OR']
    odds_ratios = np.exp(conf)

    return model, odds_ratios



def adjusted_logr(df, outcome_col='medication_compliance',
                  predictor_col='dynamic_pdc', covariates=None):
    """
    Runs an adjusted logistic regression with a binary outcome
    and continuous predictor plus additional covariates.

    Parameters:
    - df: pandas DataFrame
    - outcome_col: str, name of binary outcome column ('good' = 1, 'poor' = 0)
    - predictor_col: str, name of continuous predictor column
    - covariates: list of str, list of additional covariates to include in the model (optional)

    Returns:
    - model: fitted statsmodels logistic regression model
    - odds_ratios: DataFrame with ORs and 95% CI
    """
    # Copy dataframe to avoid modifying original
    df = df.copy()

    # Convert outcome to binary (assumes 'good' is positive class)
    df['outcome_binary'] = df[outcome_col].map({'good': 1, 'poor': 0})

    # Drop missing values in required columns
    columns_to_check = [predictor_col, 'outcome_binary'] + (covariates if covariates else [])
    df = df[columns_to_check].dropna()

    # Create formula for the model
    formula = f'outcome_binary ~ {predictor_col}'

    # Add covariates to the formula if provided
    if covariates:
        formula += ' + ' + ' + '.join(covariates)

    # Fit the model
    model = smf.logit(formula, data=df).fit(disp=False)

    # Calculate odds ratios with 95% CI
    params = model.params
    conf = model.conf_int()
    conf['OR'] = params
    conf.columns = ['2.5%', '97.5%', 'OR']
    odds_ratios = np.exp(conf)

    return model, odds_ratios



def multilevel_unadjusted_logr(df, outcome_col='medication_compliance',
                               predictor_col='dynamic_pdc', cluster_col='person_id'):
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

    # Convert outcome to binary
    df['outcome_binary'] = df[outcome_col].map({'good': 1, 'poor': 0})

    # Drop missing values
    df = df[[predictor_col, 'outcome_binary', cluster_col]].dropna()

    # Fit GEE logistic model with clustering
    model = smf.gee(f'outcome_binary ~ {predictor_col}',
                    groups=cluster_col,
                    data=df,
                    family=sm.families.Binomial()).fit()

    # Calculate odds ratios with 95% CI
    params = model.params
    conf = model.conf_int()
    conf['OR'] = params
    conf.columns = ['2.5%', '97.5%', 'OR']
    odds_ratios = np.exp(conf)

    return model, odds_ratios

def multilevel_adjusted_logr(df, outcome_col='medication_compliance', predictor_col='dynamic_pdc',
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

    # Convert outcome to binary
    df['outcome_binary'] = df[outcome_col].map({'good': 1, 'poor': 0})

    # Prepare columns to keep
    columns_to_check = [predictor_col, 'outcome_binary',
                        cluster_col] + (covariates if covariates else [])
    df = df[columns_to_check].dropna()

    # Build the formula
    formula = f'outcome_binary ~ {predictor_col}'
    if covariates:
        formula += ' + ' + ' + '.join(covariates)

    # Fit GEE logistic model with clustering
    model = smf.gee(formula=formula,
                    groups=cluster_col,
                    data=df,
                    family=sm.families.Binomial()).fit()

    # Odds Ratios and 95% CI
    params = model.params
    conf = model.conf_int()
    conf['OR'] = params
    conf.columns = ['2.5%', '97.5%', 'OR']
    odds_ratios = np.exp(conf)

    return model, odds_ratios



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

    # Prepare the outcome column
    df['outcome_binary'] = df[outcome_col].map({'good': 1, 'poor': 0})

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

    # Unadjusted Logistic Regression for each PDC column
    unadjusted_results = []
    for pdc_col in pdc_cols:

        model, odds_ratios = unadjusted_logr(df, outcome_col=outcome_col, predictor_col=pdc_col)
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
        selected_covariates = ['total_pre_exposure_days'] if 'pre' in pdc_col else ['total_post_exposure_days'] if 'post' in pdc_col else covariates or []

        model, odds_ratios = adjusted_logr(df, outcome_col=outcome_col, predictor_col=pdc_col, covariates=selected_covariates)
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

        model, odds_ratios = multilevel_unadjusted_logr(df, outcome_col=outcome_col, predictor_col=pdc_col, cluster_col=cluster_col)
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
        selected_covariates = ['total_pre_exposure_days'] if 'pre' in pdc_col else ['total_post_exposure_days'] if 'post' in pdc_col else covariates or []

        model, odds_ratios = multilevel_adjusted_logr(df, outcome_col=outcome_col, predictor_col=pdc_col, covariates=selected_covariates, cluster_col=cluster_col)
        model_result = compute_odds_ratios(model)
        model_result['pdc_col'] = pdc_col
        model_result['model'] = 'multilevel_adjusted'
        model_result['covariates'] = ', '.join(selected_covariates)
        multilevel_adjusted_results.append(model_result)

    multilevel_adjusted_results_df = pd.concat(multilevel_adjusted_results)
    save_results_to_csv(multilevel_adjusted_results_df, model_name="multilevel_adjusted")


def unadjusted_linr(df, predictor_col, outcome_col):
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
