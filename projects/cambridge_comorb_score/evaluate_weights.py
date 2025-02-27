import pandas as pd
from dotenv import load_dotenv
from lifelines.utils import concordance_index

from phmlondon.snow_utils import SnowflakeConnection

# CMM feature names
features = [
    "AGE_SQUARED",
    "SEX",
    "ALCOHOLPROBLEMS",
    "ANXIETYORDEPRESSION",
    "ATRIALFIBRILLATION",
    "CANCERINTHELAST5YEARS",
    "CHRONICKIDNEYDISEASE",
    "CHRONICLIVERDISEASEANDVIRALHEPATITIS",
    "CONSTIPATION",
    "COPD",
    "DEMENTIA",
    "DIABETES",
    "DISORDEROFPROSTATE",
    "EPILEPSY",
    "HEARTFAILURE",
    "IRRITABLEBOWELSYNDROME",
    "LEARNINGDISABILITY",
    "MULTIPLESCLEROSIS",
    "PAINFULCONDITION",
    "PARKINSONISM",
    "PERIPHVASCDISEASELEG",
    "PSYCHOACTIVESUBSTANCEMISUSE",
    "SCHIZOPHRENIAORBIPOLARDISORDER",
]
# CMM to segmentation equivalents
ltc_dict = {
    "ALCOHOLPROBLEMS": "HEALTHY_WITH_RISK_FACTORS_ALCOHOL_DEPENDENCE",
    "ANXIETYORDEPRESSION": "HEALTHY_WITH_RISK_FACTORS_DEPRESSION",
    "ATRIALFIBRILLATION": "LTC_ATRIAL_FIBRILLATION",
    "CANCERINTHELAST5YEARS": "LTC_CANCER",
    "CHRONICKIDNEYDISEASE": "LTC_CHRONIC_KIDNEY_DISEASE",
    "CHRONICLIVERDISEASEANDVIRALHEPATITIS": "LTC_CHRONIC_LIVER_DISEASE",
    "CONSTIPATION": "CONSTIPATION",  # No seg equivalent
    "COPD": "LTC_CHRONIC_OBSTRUCTIVE_PULMONARY_DISEASE",
    "DEMENTIA": "FRAILTY_AND_DEMENTIA_DEMENTIA",
    "DIABETES": "LTC_DIABETES",
    "DISORDEROFPROSTATE": "DISORDEROFPROSTATE",  # No seg equivalent
    "EPILEPSY": "LTC_EPILEPSY",
    "HEARTFAILURE": "LTC_HEART_FAILURE",
    "IRRITABLEBOWELSYNDROME": "LTC_INFLAMMATORY_BOWEL_DISEASE",
    "LEARNINGDISABILITY": "DISABILITY_LEARNING_DISABILITY",
    "MULTIPLESCLEROSIS": "LTC_MULTIPLE_SCLEROSIS",
    "PAINFULCONDITION": "LTC_CHRONIC_PAIN",
    "PARKINSONISM": "LTC_PARKINSONS_DISEASE",
    "PERIPHVASCDISEASELEG": "LTC_PERIPHERAL_VASCULAR_DISEASE",
    "PSYCHOACTIVESUBSTANCEMISUSE": "SUBSTANCE_DEPENDENCE",
    "SCHIZOPHRENIAORBIPOLARDISORDER": "SMI_SERIOUS_MENTAL_ILLNESS",
    # Include demog cols for adjusted scores
    "AGE_SQUARED": "AGE_SQUARED",
    "SEX": "SEX",
}


def set_index_date(session, date: str = "2019-06-30"):
    query = """SET index_date = '2019-06-30';"""
    session.execute_query(query)


def load_flags(session):
    query = open("projects/cambridge_comorb_score/sql_scripts/cmm_extract.sql").read()
    df = session.execute_query_to_df(query)

    # fill missing flags with 0 for CMM and Segmentation cols
    df[features] = df[features].fillna(0)
    df[[ltc_dict[i] for i in features]] = df[[ltc_dict[i] for i in features]].fillna(0)
    return df


def load_weights(session):
    """
    Loads CMM weights taken from Appendix 2
    """
    query = """
    SELECT *
    FROM intelligence_dev.ai_centre_dev.cambridge_comorb_2022_weights
    """
    weights = session.execute_query_to_df(query)
    return weights


def calculate_cmm_score(df, weight_dict, suffix):
    """
    Scores = mean sum of all weighted features
    Unadjusted excludes age and sex weights
    """
    df[f"CMM_score_{suffix}"] = sum(
        df[col.upper()] * weight for col, weight in weight_dict.items()
    ) / len(weight_dict)
    df[f"CMM_score_unadj_{suffix}"] = sum(
        df[col.upper()] * weight for col, weight in weight_dict.items() if col not in ["AGE", "SEX"]
    ) / (len(weight_dict) - 2)
    return df


def evaluate_scores(df, model):
    """
    Loops over all evaluation metrics and all time periods and calculates C-index for both adjusted
    and unadjusted scores
    """
    evals = {}

    metrics = ["death", "adm", "appt"]
    times = ["5yr", "1yr"]

    for metric in metrics:
        for year in times:
            evals[f"{metric}_{year}_adj_{model}"] = concordance_index(
                df[f"TIME_TO_{metric.upper()}_{year.upper()}"],
                -df[f"CMM_score_{model}"],
                df[f"{metric.upper()}_{year.upper()}"],
            )
            evals[f"{metric}_{year}_unadj_{model}"] = concordance_index(
                df[f"TIME_TO_{metric.upper()}_{year.upper()}"],
                -df[f"CMM_score_unadj_{model}"],
                df[f"{metric.upper()}_{year.upper()}"],
            )

    return pd.Series(evals)


if __name__ == "__main__":
    load_dotenv()

    snowsesh = SnowflakeConnection()
    snowsesh.use_database("INTELLIGENCE_DEV")
    snowsesh.use_schema("AI_CENTRE_DEV")

    set_index_date(snowsesh)

    df = load_flags(snowsesh)
    weights = load_weights(snowsesh)

    ## CMM scores - Using flags from model codesets
    cmm_weight_dict = dict(zip(weights["cond_var_name"], weights["weights"], strict=False))
    df = calculate_cmm_score(df, cmm_weight_dict, "cmm")
    cmm_evals = evaluate_scores(df, "cmm")

    ## Segmentation scores
    seg_weight_dict = dict(
        zip(
            [ltc_dict[i.upper()] for i in weights["cond_var_name"]],
            weights["weights"],
            strict=False,
        )
    )  # Take the segmentation equiv from ltc_dict
    df = calculate_cmm_score(df, seg_weight_dict, "seg")
    seg_evals = evaluate_scores(df, "seg")

    ## Combined scores
    for i in features:
        # Boolean flag for ltc if patient meets CMM or segmentation definition
        df[f"{i}_comb"] = (df[i] == 1) | (df[ltc_dict[i]] == 1)

    comb_weight_dict = dict(
        zip([i + "_comb" for i in weights["cond_var_name"]], weights["weights"], strict=False)
    )  # Take the segmentation equiv from ltc_dict
    df = calculate_cmm_score(df, seg_weight_dict, "comb")
    comb_evals = evaluate_scores(df, "comb")

    evals = pd.concat([cmm_evals, seg_evals, comb_evals])
    evals.to_csv("projects/cambridge_comorb_score/results/cmm_c-indices.csv")
