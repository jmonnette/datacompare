import pandas as pd
import numpy as np

# Create the first DataFrame
df1 = pd.DataFrame({
    'A': np.random.randint(0, 5, 1000),
    'B': np.random.randint(0, 5, 1000),
    'C': np.random.randint(0, 5, 1000),
})

# Write the first DataFrame to a csv
df1.to_csv('data1.csv', index=False)

# Create the second DataFrame with some changes
df2 = df1.copy()
change_indices = np.random.choice(df2.index, replace=False, size=int(df2.shape[0]*0.1)) # Change 10% of data
df2.loc[change_indices, 'A'] = df2.loc[change_indices, 'A'].apply(lambda x: (x+1)%5)
df2.loc[change_indices, 'B'] = df2.loc[change_indices, 'B'].apply(lambda x: (x+1)%5)
df2.loc[change_indices, 'C'] = df2.loc[change_indices, 'C'].apply(lambda x: (x+1)%5)

# Write the second DataFrame to a csv
df2.to_csv('data2.csv', index=False)