import numpy as np
import statsmodels.api as sm
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

