import pandas as pd
import json
from sklearn.ensemble import IsolationForest

# This module contains functions to compare two dataframes.
# Key assumptions made about the input dataframes:
# - Row counts are the same in both dataframes
# - Rows are in the same order
# - Columns have the same names and data types
# Any deviation from these assumptions will be identified and reported in the program output,
# therefore it is important to ensure these assumptions are fulfilled before using these comparison functions.

def _read_csv(filepath):
    """
    This function reads a csv file into a pandas DataFrame and replaces empty strings with NaN values.
    """
    try:
        df = pd.read_csv(filepath)
        df.replace({'': pd.NA}, inplace=True)
        return df
    except FileNotFoundError:
        raise ValueError(f"No file found at path '{filepath}'")

def _ensure_same_shape(df1, df2):
    """
    This function ensures the two dataframes have the same shape & dtypes.
    It aligns columns, checks that the number of rows and dtypes is the same for both dataframes.
    """
    new_cols_in_df1 = []
    new_cols_in_df2 = []

    # Check if both dataframes have the same columns
    if set(df1.columns) != set(df2.columns):
        common_cols = set(df1.columns).intersection(set(df2.columns))
        missing_cols_df1 = set(df2.columns) - common_cols
        missing_cols_df2 = set(df1.columns) - common_cols

        # Add missing columns filled with NaN
        for col in missing_cols_df1:
            df1[col] = pd.NA
            new_cols_in_df1.append(col)
        for col in missing_cols_df2:
            df2[col] = pd.NA
            new_cols_in_df2.append(col)

    # Ensure same column order in both dataframes
    df1 = df1[df2.columns]

    # Check if both dataframes have the same dtypes at the same column positions
    if not all(df1.dtypes == df2.dtypes):
        mismatches = (df1.dtypes != df2.dtypes)
        mismatch_cols = mismatches[mismatches].index.tolist()
        raise ValueError(f'The following columns have different dtypes in the dataframes: {mismatch_cols}')

    # Check if both dataframes have the same number of rows
    if len(df1) != len(df2):
        raise ValueError(f"The two dataframes have a different number of rows. DataFrame 1 has {len(df1)} rows while DataFrame 2 has {len(df2)} rows.")

    return df1, df2, new_cols_in_df1, new_cols_in_df2

def _calculate_outliers(df, col):
    """
    This function calculates outliers in the given column using IQR methodology.
    """
    Q1 = df[col].quantile(0.25)
    Q3 = df[col].quantile(0.75)
    IQR = Q3 - Q1
    outliers = df[(df[col] < (Q1 - 1.5 * IQR)) |
                  (df[col] > (Q3 + 1.5 * IQR))][col]
    return outliers

def _calculate_changes_summary(df1, df2, col):
    """
    This function calculates the changes from df1 to df2 in the given column,
    considering the change from value to NaN or from NaN to value as a change as well.
    """
    changes_df = pd.DataFrame()
    mask = pd.isna(df1[col]) ^ pd.isna(df2[col])
    changes_df['from'] = df1.loc[mask | (df1[col] != df2[col]), col]
    changes_df['to'] = df2.loc[mask | (df1[col] != df2[col]), col]

    # Replace NaN values with a placeholder
    changes_df.fillna('NaN', inplace=True)

    changes_summary = changes_df.groupby(['from', 'to']).size().reset_index().rename(columns={0:'count'}).sort_values(by='count', ascending=False)[:5]

    # Replace the placeholder back with nan in the 'from' and 'to' columns
    changes_summary.replace('NaN', pd.NA, inplace=True)

    changes_summary_json = json.dumps(changes_summary.to_dict(orient='records'))

    return changes_summary_json

def _compare_dataframes(df1, df2):
    """
    This function compares the two given dataframes column by column.
    """
    column_diffs = []

    for col in df1.columns:
        col_diff = (df1[col] != df2[col]).sum()
        per_diff = col_diff / len(df1) * 100
        changes_summary_json = _calculate_changes_summary(df1, df2, col)

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

            outliers_df1 = _calculate_outliers(df1, col).to_json()
            outliers_df2 = _calculate_outliers(df2, col).to_json()
        else:
            median_df1 = median_df2 = mean_df1 = mean_df2 = std_df1 = std_df2 = sum_df1 = sum_df2 = None
            outliers_df1 = outliers_df2 = None

        column_diffs.append([col, col_diff, per_diff, changes_summary_json, non_null_rows_df1, non_null_rows_df2, distinct_values_df1, distinct_values_df2, median_df1, median_df2, mean_df1, mean_df2, std_df1, std_df2, sum_df1, sum_df2, top_values_df1, top_values_df2, outliers_df1, outliers_df2])

    return column_diffs

def _diff_rows(df1, df2):
    """
    This function compares two dataframes row-wise.
    """
    row_diffs = pd.DataFrame()
    row_diffs['All_isequal'] = (df1 == df2).all(axis=1)

    for col in df1.columns:
        row_diffs[col+'_df1'] = df1[col]
        row_diffs[col+'_df2'] = df2[col]
        row_diffs[col+'_isequal'] = df1[col] == df2[col]

    return row_diffs

def _check_column_presence(columns, new_cols_in_df1, new_cols_in_df2):
    """
    Check if a column existed in both input datasets or only in one
    """
    column_presence = []
    for column in columns:
        if column in new_cols_in_df1:
            column_presence.append('Added to DF1')
        elif column in new_cols_in_df2:
            column_presence.append('Added to DF2')
        else:
            column_presence.append('Exists in both')

    return column_presence

def _load_datasets(df1, df2):
    if not isinstance(df1, (str, pd.DataFrame)) or not isinstance(df2, (str, pd.DataFrame)):
        raise ValueError("Input arguments must be either two strings or two pandas.DataFrame instances.")

    # Load CSV file if string is given
    if isinstance(df1, str):
        df1 = _read_csv(df1)
    if isinstance(df2, str):
        df2 = _read_csv(df2)

    return df1, df2

def compare_datasets(df1, df2):
    """
    This function checks the type of the input and compares the two input datasets.
    """
    df1, df2 = _load_datasets(df1, df2)
    df1, df2, new_cols_in_df1, new_cols_in_df2 = _ensure_same_shape(df1, df2)

    column_diffs = _compare_dataframes(df1, df2)
    col_summary = pd.DataFrame(column_diffs, columns=[
        'Column',
        'Number of Differences',
        'Percentage of Differences',
        'Top 5 Changes',
        'Non-null Rows DF1',
        'Non-null Rows DF2',
        'Distinct Values DF1',
        'Distinct Values DF2',
        'Median DF1',
        'Median DF2',
        'Mean DF1',
        'Mean DF2',
        'Std Dev DF1',
        'Std Dev DF2',
        'Sum DF1',
        'Sum DF2',
        'Top 5 values DF1',
        'Top 5 values DF2',
        'Outliers DF1',
        'Outliers DF2'])
    col_summary['Column Presence'] = _check_column_presence(df1.columns, new_cols_in_df1, new_cols_in_df2)

    row_diffs = _diff_rows(df1, df2)

    return col_summary, row_diffs

def detect_anomalies(df1, df2, column_list=None, contamination=0.005):
    df1, df2 = _load_datasets(df1, df2)

    # Making sure we're working with copies and don't modify original dataframes
    df1 = df1.copy()
    df2 = df2.copy()

    columns = []

    # Validate and process columns argument
    if column_list is None:
        # Select only numeric columns from df1
        columns = df1.select_dtypes(include=['float64', 'int64']).columns
    else:
        columns = df1[column_list].columns

    # Ensure all provided columns exist in the dataframes and are numeric
    for column in columns:
        if column not in df1.columns or column not in df2.columns:
            raise ValueError(f"Column '{column}' not found in one or both dataframes.")
        if df1[column].dtype not in ['int64', 'float64'] or df2[column].dtype not in ['int64', 'float64']:
            raise ValueError(f"Column '{column}' is not numeric in one or both dataframes.")

    df1 = df1[columns]
    df2 = df2[columns]

    # Handle missing values
    df1.dropna(inplace=True)
    df2.dropna(inplace=True)

    # Initialize the Model
    iso = IsolationForest(contamination=contamination)

    # Fit and Predict
    iso.fit(df1)
    df2['anomaly'] = iso.predict(df2)

    # Filter and return only the anomalies from df2
    anomalies = df2[df2['anomaly'] == -1]

    return anomalies
