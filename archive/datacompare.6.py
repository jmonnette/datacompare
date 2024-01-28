import pandas as pd
import json

def _calculate_outliers(df, col):
    Q1 = df[col].quantile(0.25)
    Q3 = df[col].quantile(0.75)
    IQR = Q3 - Q1
    outliers = df[(df[col] < (Q1 - 1.5 * IQR)) | (df[col] > (Q3 + 1.5 * IQR))][col]
    return outliers

def _compare_dataframes(df1, df2):
    column_diffs = []

    for col in df1.columns:
        col_diff = (df1[col] != df2[col]).sum()
        per_diff = col_diff / len(df1) * 100

        changes_df = pd.DataFrame()
        changes_df['from'] = df1.loc[df1[col] != df2[col], col]
        changes_df['to'] = df2.loc[df1[col] != df2[col], col]

        changes_summary = changes_df.groupby(['from', 'to']).size().reset_index().rename(columns={0: 'count'}).sort_values(by='count', ascending=False)[:5]
        changes_summary_json = changes_summary.to_json(orient='records')

        non_null_rows_df1 = df1[col].count()
        non_null_rows_df2 = df2[col].count()

        distinct_values_df1 = df1[col].nunique()
        distinct_values_df2 = df2[col].nunique()

        top_values_df1 = df1[col].value_counts().nlargest(5).to_dict()
        top_values_df2 = df2[col].value_counts().nlargest(5).to_dict()

        if pd.api.types.is_numeric_dtype(df1[col]):
            median_df1 = df1[col].median()
            mean_df1 = df1[col].mean()
            std_df1 = df1[col].std()
            sum_df1 = df1[col].sum()

            median_df2 = df2[col].median()
            mean_df2 = df2[col].mean()
            std_df2 = df2[col].std()
            sum_df2 = df2[col].sum()

            outliers_df1 = _calculate_outliers(df1, col)
            outliers_df2 = _calculate_outliers(df2, col)
            
            # convert outliers to json
            outliers_df1_json = outliers_df1.to_json()
            outliers_df2_json = outliers_df2.to_json()

        else:
            median_df1 = median_df2 = mean_df1 = mean_df2 = std_df1 = std_df2 = sum_df1 = sum_df2 = None
            outliers_df1_json = outliers_df2_json = None

        column_diffs.append([col, col_diff, per_diff, changes_summary_json, non_null_rows_df1, non_null_rows_df2, distinct_values_df1, distinct_values_df2, median_df1, median_df2, mean_df1, mean_df2, std_df1, std_df2, sum_df1, sum_df2, top_values_df1, top_values_df2, outliers_df1_json, outliers_df2_json])

    return column_diffs

def _diff_rows(df1, df2):
    row_diffs = pd.DataFrame()

    for col in df1.columns:
        row_diffs[col+'_df1'] = df1[col]
        row_diffs[col+'_df2'] = df2[col]
        row_diffs[col+'_isequal'] = df1[col] == df2[col]
    
    return row_diffs

def compare_datasets(df1, df2):
    if not isinstance(df1, (str, pd.DataFrame)) or not isinstance(df2, (str, pd.DataFrame)):
        raise ValueError("Input arguments must be either two strings or two pandas.DataFrame instances.")

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

    column_diffs = _compare_dataframes(df1, df2)
    col_summary = pd.DataFrame(column_diffs, columns=['Column', 'Number of Differences', 'Percentage of Differences', 'Top 5 Changes', 'Non-null Rows DF1', 'Non-null Rows DF2', 'Distinct Values DF1', 'Distinct Values DF2', 'Median DF1', 'Median DF2', 'Mean DF1', 'Mean DF2', 'Std Dev DF1', 'Std Dev DF2', 'Sum DF1', 'Sum DF2', 'Top 5 values DF1', 'Top 5 values DF2', 'Outliers DF1', 'Outliers DF2'])

    row_diffs = _diff_rows(df1, df2)
    
    return col_summary, row_diffs