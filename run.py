import datacompare as dc
import pandas as pd
from sklearn.model_selection import train_test_split


col_summary, row_diffs = dc.compare_datasets('data1.csv', 'data2.csv')

print('Finsihed comparing datasets')

# Save output data sets
col_summary.to_csv('column_summary.csv', index=False)
row_diffs.to_csv('row_differences.csv', index=False)

df = pd.read_csv('obesity_data.csv')

train, test = train_test_split(df, test_size=.5, random_state=37)

anomalies = dc.detect_anomalies(test, train, ['Weight'])
anomalies.to_csv('anolmalies_A.csv', index=False)
