import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
import statsmodels.formula.api as smf
from statsmodels.api import add_constant
from statsmodels.regression.mixed_linear_model import MixedLM
from statsmodels.stats.outliers_influence import variance_inflation_factor


def check_linearity_log_odds(df, outcome_col, continuous_predictors):
    """
    Checks linearity of log odds using predicted probabilities from logistic regression
    and plots them against each continuous predictor.
    """
    df = df.copy()
    if df[outcome_col].dtype == 'O':
        df[outcome_col] = df[outcome_col].map({'good': 1, 'poor': 0})

    # Fit basic logistic model with just the continuous predictors
    predictors_str = ' + '.join(continuous_predictors)
    formula = f"{outcome_col} ~ {predictors_str}"
    model = smf.logit(formula, data=df).fit(disp=False)

    # Predict probabilities and calculate log-odds
    df['predicted_prob'] = model.predict(df)
    df['logit'] = np.log(df['predicted_prob'] / (1 - df['predicted_prob']))

    # Plot smoothed log-odds vs each continuous predictor
    for predictor in continuous_predictors:
        sns.regplot(x=predictor, y='logit', data=df, lowess=True,
                    scatter_kws={'alpha': 0.2}, line_kws={'color': 'red'})
        plt.title(f'Smoothed Log-Odds vs {predictor}')
        plt.ylabel('Log-Odds (Predicted)')
        plt.xlabel(predictor)
        plt.tight_layout()
        plt.show()


def check_vif(df, predictors):
    """
    Calculates Variance Inflation Factor (VIF) for predictors to check multicollinearity.

    Parameters:
    - df: pandas DataFrame
    - predictors: list of str, predictor column names

    Returns:
    - vif_df: DataFrame with VIFs
    """
    X = add_constant(df[predictors].dropna())
    vif_df = pd.DataFrame({
        "Variable": X.columns,
        "VIF": [variance_inflation_factor(X.values, i) for i in range(X.shape[1])]
    })
    return vif_df


def check_within_cluster_variation(df, cluster_col, outcome_col):
    """
    Checks whether there's variation in outcome within clusters.

    Parameters:
    - df: pandas DataFrame
    - cluster_col: str, grouping variable (e.g. person_id)
    - outcome_col: str, binary outcome column

    Returns:
    - summary: DataFrame showing distribution of unique outcome values per cluster
    """
    counts = df.groupby(cluster_col)[outcome_col].nunique()
    summary = counts.value_counts().sort_index().rename_axis('Unique outcome values in group'
                                                             ).reset_index(name='Num groups')
    return summary


def estimate_icc(df, outcome_col, cluster_col):
    """
    Estimates the intra-class correlation coefficient (ICC)
      for a binary outcome using a random intercept model.

    Parameters:
    - df: pandas DataFrame
    - outcome_col: str
    - cluster_col: str

    Returns:
    - ICC estimate (float)
    """
    df = df.copy()
    df[outcome_col] = df[outcome_col].map({'good': 1, 'poor': 0}) if df[outcome_col
                                                                ].dtype == 'O' else df[outcome_col]

    model = MixedLM.from_formula(f"{outcome_col} ~ 1", groups=cluster_col, data=df)
    result = model.fit()
    var_components = result.cov_re.iloc[0, 0]
    residual = result.scale
    icc = var_components / (var_components + residual)
    return icc


def plot_residuals_vs_fitted(fitted_model, df, outcome_col):
    """
    Plots residuals vs. fitted values for basic diagnostic.

    Parameters:
    - fitted_model: fitted statsmodels model object
    - df: pandas DataFrame used in the model
    - outcome_col: str
    """
    pred_probs = fitted_model.predict(df)
    residuals = df[outcome_col] - pred_probs

    plt.figure(figsize=(8, 5))
    sns.scatterplot(x=pred_probs, y=residuals, alpha=0.3)
    plt.axhline(0, color='red', linestyle='--')
    plt.xlabel('Fitted probabilities')
    plt.ylabel('Residuals')
    plt.title('Residuals vs Fitted Values')
    plt.show()
