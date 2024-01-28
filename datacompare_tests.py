import unittest
import pandas as pd
import json
import os

class DataFrameCompareTests(unittest.TestCase):

    def setUp(self):
        self.test_file_path1 = 'data1.csv'
        self.test_file_path2 = 'data2.csv'
        self.df1 = pd.DataFrame(data={'A': [1, 2, 3], 'B': [4, 5, 6]})
        self.df2 = pd.DataFrame(data={'A': [1, 2, 0], 'B': [4, 0, 6]})
        self.df1.to_csv(self.test_file_path1, index=False)
        self.df2.to_csv(self.test_file_path2, index=False)

    def tearDown(self):
        os.remove(self.test_file_path1)
        os.remove(self.test_file_path2)

    def test_ensure_same_shape(self):
         df1, df2, _, _ = _ensure_same_shape(self.df1, self.df2)
         self.assertEqual(df1.shape, df2.shape)

    def test_read_csv(self):
        df1 = _read_csv(self.test_file_path1)
        self.assertTrue(df1.equals(self.df1))

    def test_check_column_presence(self):
        cols = _check_column_presence(self.df1.columns, [], [])
        self.assertIsInstance(cols, list)
        self.assertTrue(all(x == 'Exists in both' for x in cols))

    def test_compare_dataframes(self):
        diffs = _compare_dataframes(self.df1, self.df2)
        self.assertIsInstance(diffs, list)
        self.assertNotEqual(len(diffs), 0)

    def test_compare_datasets(self):
        col_summary, row_diffs = compare_datasets(self.test_file_path1, self.test_file_path2)
        self.assertIsInstance(col_summary, pd.DataFrame)
        self.assertIsInstance(row_diffs, pd.DataFrame)

    def test_detect_anomalies(self):
        anomalies = detect_anomalies(self.test_file_path1, self.test_file_path2)
        self.assertIsInstance(anomalies, pd.DataFrame)

if __name__ == '__main__':
    unittest.main()