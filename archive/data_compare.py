import pandas as pd

# read your data sets
data1 = pd.read_csv('data1.csv')
data2 = pd.read_csv('data2.csv')

column_diffs = []
for col in data1.columns:
    col_diff = (data1[col] != data2[col]).sum()
    per_diff = col_diff / len(data1) * 100
    changes = data1[data1[col] != data2[col]][col].value_counts()[:5]
    column_diffs.append([col, col_diff, per_diff, changes])

# Create the first output dataset
col_summary = pd.DataFrame(column_diffs, columns=['Column', 'Number of Differences', 'Percentage of Differences', 'Top 5 Changes'])
col_summary.to_csv('column_summary.csv', index=False)

# Create the row differences dataframe
row_diffs = data1.copy()
for col in data1.columns:
    row_diffs[col] = data1[col] != data2[col]
row_diffs.to_csv('row_differences.csv', index=False)