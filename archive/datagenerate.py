import pandas as pd
import numpy as np
import random
import string

# Create some text values for new columns
def random_string(length):
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for i in range(length))

colDVals = [random_string(10) for _ in range(10)]
colEVals = [random_string(10) for _ in range(5)]

# Create the first DataFrame
df1 = pd.DataFrame({
    'A': np.random.randint(0, 1000, 10000),
    'B': np.random.randint(0, 1000, 10000),
    'C': np.random.randint(0, 1000, 10000),
    'D': [random.choice(colDVals) for _ in range(10000)],
    'E': [random.choice(colEVals) for _ in range(10000)],
})

# Write the first DataFrame to a csv
df1.to_csv('data1.csv', index=False)

# Create the second DataFrame with some changes
df2 = df1.copy()
change_indices = np.random.choice(df2.index, replace=False, size=int(df2.shape[0]*0.1))  # Change 10% of data
df2.loc[change_indices, ['A','B','C']] = df2.loc[change_indices, ['A','B','C']].apply(lambda x: (x+1)%1000)
df2.loc[change_indices, ['D']] = df2.loc[change_indices, ['D']].applymap(lambda x: random.choice(colDVals))
df2.loc[change_indices, ['E']] = df2.loc[change_indices, ['E']].applymap(lambda x: random.choice(colEVals))

# Replace some values with NaN
nan_indices = np.random.choice(df2.index, replace=False, size=int(df2.shape[0]*0.1))  # 10% of row indices
for col in df2.columns:
    df2.loc[nan_indices, col] = np.nan

# Write the second DataFrame to a csv
df2.to_csv('data2.csv', index=False)
