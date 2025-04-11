import numpy as np
import pandas as pd
import statsmodels.formula.api as smf


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



def adjusted_logr(df, outcome_col='medication_compliance', predictor_col='dynamic_pdc', covariates=None):
    """
    Runs an adjusted logistic regression with a binary outcome and continuous predictor plus additional covariates.

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
