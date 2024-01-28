import pandas as pd

# Function for calculating outliers based on IQR
def _calculate_outliers(df, col):
    # Computing IQR
    Q1 = df[col].quantile(0.25)
    Q3 = df[col].quantile(0.75)
    IQR = Q3 - Q1

    # Identifying outliers
    outliers = df[(df[col] < (Q1 - 1.5 * IQR)) | (df[col] > (Q3 + 1.5 * IQR))][col]
    return outliers

# Function for comparing two dataframes
def _compare_dataframes(df1, df2):
    # List to hold the comparison results for each column
    column_diffs = []

    # Check each column in df1
    for col in df1.columns:
        # Compare columns in df1 and df2
        col_diff = (df1[col] != df2[col]).sum()
        per_diff = col_diff / len(df1) * 100

        # Identify and summarize changes
        changes = df1.loc[df1[col] != df2[col], col].value_counts().index.tolist()
        changes_to = df2.loc[df1[col] != df2[col], col].value_counts().index.tolist()
        changes_summary = [(from_val, to_val, count) for from_val, to_val, count in zip(changes, changes_to, range(col_diff))]
        changes_summary = sorted(changes_summary, key=lambda x: x[2], reverse=True)[:5]

        # Calculate number of non-null rows and distinct values in the columns
        non_null_rows_df1 = df1[col].count()
        non_null_rows_df2 = df2[col].count()
        distinct_values_df1 = df1[col].nunique()
        distinct_values_df2 = df2[col].nunique()

        # Identify top 5 values for non-numerical columns
        top_values_df1 = df1[col].value_counts().nlargest(5).to_dict()
        top_values_df2 = df2[col].value_counts().nlargest(5).to_dict()

        # If column is numerical, calculate some summary statistics
        if pd.api.types.is_numeric_dtype(df1[col]):
            median_df1 = df1[col].median()
            mean_df1 = df1[col].mean()
            std_df1 = df1[col].std()
            sum_df1 = df1[col].sum()

            median_df2 = df2[col].median()
            mean_df2 = df2[col].mean()
            std_df2 = df2[col].std()
            sum_df2 = df2[col].sum()

            # Get outliers for numerical columns
            outliers_df1 = _calculate_outliers(df1, col)
            outliers_df2 = _calculate_outliers(df2, col)
        else:
            # If not numerical, set numerical values to None
            median_df1 = median_df2 = mean_df1 = mean_df2 = std_df1 = std_df2 = sum_df1 = sum_df2 = None
            outliers_df1 = outliers_df2 = None

        # Add column comparison results to column_diffs list
        column_diffs.append([col, col_diff, per_diff, changes_summary, non_null_rows_df1, non_null_rows_df2, distinct_values_df1, distinct_values_df2, median_df1, median_df2, mean_df1, mean_df2, std_df1, std_df2, sum_df1, sum_df2, top_values_df1, top_values_df2, outliers_df1, outliers_df2])

    return column_diffs

# Function for comparing rows of two dataframes
def _diff_rows(df1, df2):
    row_diffs = pd.DataFrame()

    for col in df1.columns:
        row_diffs[col+'_df1'] = df1[col]
        row_diffs[col+'_df2'] = df2[col]
        row_diffs[col+'_isequal'] = df1[col] == df2[col]

    return row_diffs

# Main function for comparing two datasets
def compare_datasets(df1, df2):
    # Check if input arguments are valid
    if not isinstance(df1, (str, pd.DataFrame)) or not isinstance(df2, (str, pd.DataFrame)):
        raise ValueError("Input arguments must be either two strings or two pandas.DataFrame instances.")

    # Load dataframes from file paths, if file paths are provided
    if isinstance(df1, str):
        try:
            df1 = pd.read_csv(df1)
        except FileNotFoundError:
            raise ValueError(f"No file found at path '{df1}'")

    if isinstance(df2, str):
        try:
            df2 = pd.read_csv(df2)
        except FileNotFoundError:
            raise ValueError(f"No file found at path '{df2}'")

    # Compare dataframes
    column_diffs = _compare_dataframes(df1, df2)
    # Create summary dataframe
    col_summary = pd.DataFrame(column_diffs, columns=['Column', 'Number of Differences', 'Percentage of Differences', 'Top 5 Changes', 'Non-null Rows DF1', 'Non-null Rows DF2', 'Distinct Values DF1', 'Distinct Values DF2', 'Median DF1', 'Median DF2', 'Mean DF1', 'Mean DF2', 'Std Dev DF1', 'Std Dev DF2', 'Sum DF1', 'Sum DF2', 'Top 5 values DF1', 'Top 5 values DF2', 'Outliers DF1', 'Outliers DF2'])

    # Compare rows
    row_diffs = _diff_rows(df1, df2)

    return col_summary, row_diffs
