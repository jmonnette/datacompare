import pandas as pd
import json

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
    This function ensures the two dataframes have the same shape.
    It aligns columns and checks that the number of rows is the same for both dataframes.
    """
    # Check if both dataframes have the same columns
    if set(df1.columns) != set(df2.columns):
        common_cols = set(df1.columns).intersection(set(df2.columns))
        missing_cols_df1 = set(df2.columns) - common_cols
        missing_cols_df2 = set(df1.columns) - common_cols

        # Add missing columns filled with NaN
        for col in missing_cols_df1:
            df1[col] = pd.NA
        for col in missing_cols_df2:
            df2[col] = pd.NA

    # Ensure same column order in both dataframes
    df1 = df1[df2.columns]

    # Check if both dataframes have the same number of rows
    if len(df1) != len(df2):
        raise ValueError("The two dataframes have a different number of rows.")

    return df1, df2

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

    changes_summary_json = changes_summary.to_json(orient='records')

    return changes_summary_json

def _compare_dataframes(df1, df2):
    """
    This function compares the two given dataframes column by column.
    """
    column_diffs = []
    # Implement the entire code here to compare the dataframes by columns
    # As it was specified before
    return column_diffs

def _diff_rows(df1, df2):
    """
    This function compares two dataframes row-wise.
    """
    row_diffs = pd.DataFrame()
    # Implement the entire code to compare the dataframes by rows
    # As it was specified before
    return row_diffs

def compare_datasets(df1, df2):
    """
    This function checks the type of the input and compares the two input datasets.
    """
    if not isinstance(df1, (str, pd.DataFrame)) or 
       not isinstance(df2, (str, pd.DataFrame)):
        raise ValueError("Input arguments must be either two strings or two pandas.DataFrame instances.")

    # Load CSV file if string is given
    if isinstance(df1, str):
        df1 = _read_csv(df1)
    if isinstance(df2, str):
        df2 = _read_csv(df2)
    
    # Ensure dataframes have the same shape
    df1, df2 = _ensure_same_shape(df1, df2)

    column_diffs = _compare_dataframes(df1, df2)
    col_summary = pd.DataFrame(column_diffs, columns=['Column', 'Number of Differences', 'Percentage of Differences', 'Top 5 Changes', 'Non-null Rows DF1', 'Non-null Rows DF2', 'Distinct Values DF1', 'Distinct Values DF2', 'Median DF1', 'Median DF2', 'Mean DF1', 'Mean DF2', 'Std Dev DF1', 'Std Dev DF2', 'Sum DF1', 'Sum DF2', 'Top 5 values DF1', 'Top 5 values DF2', 'Outliers DF1', 'Outliers DF2'])

    row_diffs = _diff_rows(df1, df2)
    
    return col_summary, row_diffs