import pandas as pd

# read your data sets
data1 = pd.read_csv('data1.csv')
data2 = pd.read_csv('data2.csv')

column_diffs = []

for col in data1.columns:
    col_diff = (data1[col] != data2[col]).sum()
    per_diff = col_diff / len(data1) * 100
    
    # Get the top 5 changes from and to values
    changes = data1.loc[data1[col] != data2[col], col].value_counts().index.tolist()
    changes_to = data2.loc[data1[col] != data2[col], col].value_counts().index.tolist()
    
    # combining from, to and counts and taking top 5
    changes_summary = [(from_val, to_val, count) for from_val, to_val, count in zip(changes, changes_to, range(col_diff))]
    changes_summary = sorted(changes_summary, key=lambda x: x[2], reverse=True)[:5]

    # Get the number of non-null rows and distinct values
    non_null_rows = data1[col].count() # non-null rows in data1
    distinct_values = data1[col].nunique()  # unique values in data1

    column_diffs.append([col, col_diff, per_diff, changes_summary, non_null_rows, distinct_values])

# Create the first output dataset
col_summary = pd.DataFrame(column_diffs, columns=['Column', 'Number of Differences', 'Percentage of Differences', 'Top 5 Changes', 'Non-null Rows', 'Distinct Values'])
col_summary.to_csv('column_summary.csv', index=False)

# Create the row differences dataframe
row_diffs = data1.copy()
for col in data1.columns:
    row_diffs[col] = data1[col] != data2[col]
row_diffs.to_csv('row_differences.csv', index=False)