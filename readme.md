# Data Comparison and Anomaly Detection Module

This module provides a set of functions for comparing two datasets and detecting anomalies. It is designed to take two input datasets and perform a detailed comparison of the columns, values, and rows. The module can also detect anomalies in one dataset based on the other using the Isolation Forest method for anomaly detection.

## Key Functions
Here are the main functions provided by the module:

### 1. `compare_datasets(df1, df2)`
This function takes as input two datasets (either as pandas DataFrame objects or as paths to CSV files) and returns a detailed comparison report in the form of two pandas DataFrames: `col_summary` and `row_diffs`.

`col_summary` includes:
- column names
- number and percentage of differences
- top 5 changes
- number of non-null rows and distinct values in both datasets
- median, mean, standard deviation, sum, top 5 values, and outliers for numeric columns

`row_diffs` contains a row-wise comparison of the two input datasets.

### 2. `detect_anomalies(df1, df2, column_list=None, contamination=0.005)`
This function takes as input two datasets (either as pandas DataFrame objects or as paths to CSV files), a list of columns to be considered for anomaly detection, and a contamination factor which is the proportion of outliers in the data. It returns anomalies detected in the second dataset based on the first dataset.

## Installation
Nothing special is needed for the installation as long as you have the necessary dependencies: `pandas` and `sklearn`.

## Usage
Here's an example:

```python
import pandas as pd
df1 = pd.DataFrame({'A': [1, 2, 3], 'B': [4, 5, 6]})
df2 = pd.DataFrame({'A': [1, 2, 0], 'B': [4, 0, 6]})
summary, row_diffs = compare_datasets(df1, df2)
print(summary)
print(row_diffs)
anomalies = detect_anomalies(df1, df2)
print(anomalies)