import matplotlib.pyplot as plt
from tableone import TableOne


def get_top_n_classes(df, column_name, n=10):
    """
    Get the top n most frequent values in a specified column of the dataframe.

    Args:
        df (pd.DataFrame): The dataframe containing the data.
        column_name (str): The name of the column to analyze.
        n (int): The number of top values to return. Default is 10.

    Returns:
        pd.Series: A series containing the top n most frequent values and their counts.
    """
    return df[column_name].value_counts().head(n)

def plot_top_classes(class_counts):
    """
    Plot the top classes in a bar graph.

    Args:
        class_counts (pd.Series): A series containing the class counts.
    """
    plt.figure(figsize=(10, 6))
    class_counts.plot(kind='bar', color='skyblue')

    # Set the title and labels
    plt.title('Top Most Frequent Classes', fontsize=16)
    plt.xlabel('Class', fontsize=12)
    plt.ylabel('Frequency', fontsize=12)

    # Rotate x-axis labels for readability
    plt.xticks(rotation=45, ha='right')

    # Adjust layout to fit everything
    plt.tight_layout()

    # Display the plot
    plt.show()

def get_table_one(df, stratification_col):
    """Creates a journal style 'tableone' with descriptive statistics
    and appropriate tests for stratified data"""

    categorical_vars = ['gender', 'ethnicity','imd','drug_class']
    continuous_vars = [ 'static_pdc', 'dynamic_pdc', 'total_exposed_days', 'age_at_start']

    table1 = TableOne(df, categorical=categorical_vars,
                  continuous=continuous_vars,
                  groupby= stratification_col,
                  pval=True, missing=True)


    print(table1)



def compare_durations_summary(comp_orders):
    """
    Compare the 'duration_days' and 'calculated_duration' and flag the most accurate one.

    Arguments:
    comp_orders : DataFrame containing 'duration_days' and 'calculated_duration'

    Returns:
    summary_stats : Dictionary containing summary statistics of the comparisone
    """

    # Calculate absolute difference between documented and calculated duration
    abs_diff = (comp_orders['duration_days'] - comp_orders['calculated_duration']).abs()
    comp_orders['abs_diff'] = abs_diff

    # Calculate relative difference (relative to the max of the two values)
    max_values = comp_orders[['duration_days', 'calculated_duration']].max(axis=1)
    rel_diff = abs_diff / max_values
    comp_orders['rel_diff'] = rel_diff

    # Calculate percentage difference
    comp_orders['perc_diff'] = (comp_orders['abs_diff'] / comp_orders['duration_days']) * 100

    summary_stats = {
        "mean_abs_diff": comp_orders['abs_diff'].mean(),
        "median_abs_diff": comp_orders['abs_diff'].median(),
        "max_abs_diff": comp_orders['abs_diff'].max(),
        "min_abs_diff": comp_orders['abs_diff'].min(),
        "mean_perc_diff": comp_orders['perc_diff'].mean(),
        "median_perc_diff": comp_orders['perc_diff'].median(),
        "max_perc_diff": comp_orders['perc_diff'].max(),
        "min_perc_diff": comp_orders['perc_diff'].min(),
        "count_flag_calculated": (comp_orders['abs_diff'] <= 5).sum(),  #Example where abs_diff <= 5
        "count_flag_documented": (comp_orders['abs_diff'] > 5).sum(),  #Example where abs_diff > 5
    }

    return summary_stats

