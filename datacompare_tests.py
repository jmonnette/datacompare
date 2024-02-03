import unittest
import pandas as pd
import json
import os
import datacompare as dc

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
         df1, df2, _, _ = dc._ensure_same_shape(self.df1, self.df2)
         self.assertEqual(df1.shape, df2.shape)

    def test_read_csv(self):
        df1 = dc._read_csv(self.test_file_path1)
        self.assertTrue(df1.equals(self.df1))

    def test_check_column_presence(self):
        cols = dc._check_column_presence(self.df1.columns, [], [])
        self.assertIsInstance(cols, list)
        self.assertTrue(all(x == 'Exists in both' for x in cols))

    def test_compare_dataframes(self):
        diffs = dc._compare_dataframes(self.df1, self.df2)
        self.assertIsInstance(diffs, list)
        self.assertNotEqual(len(diffs), 0)

    def test_compare_datasets(self):
        col_summary, row_diffs = dc.compare_datasets(self.test_file_path1, self.test_file_path2)
        self.assertIsInstance(col_summary, pd.DataFrame)
        self.assertIsInstance(row_diffs, pd.DataFrame)

    def test_detect_anomalies(self):
        anomalies = dc.detect_anomalies(self.test_file_path1, self.test_file_path2)
        self.assertIsInstance(anomalies, pd.DataFrame)

class DataFrameAssertTests(unittest.TestCase):
    def setUp(self):
        self.df1 = pd.DataFrame({'A': [1, 2, 3], 'B': [4, 5, 6]})
        self.df2 = pd.DataFrame({'A': [1, 2, 3], 'B': [4, 5, 6]})
        self.df3 = pd.DataFrame({'A': [1, 2, 3], 'B': [4, 0, 6]})
        self.summary = pd.DataFrame({
            'Column': ['A', 'B'],
            'Sum DF1': [6, 15],
            'Sum DF2': [6, 15]
        })

    def test_assert_on_rows(self):
        # this should pass, since all rows have the same sum
        dc.assert_on_rows(self.df1, self.df2, lambda x, y: x.sum() == y.sum())

        # this should fail, since the second row has a different sum in df3
        with self.assertRaises(AssertionError):
            dc.assert_on_rows(self.df1, self.df3, lambda x, y: x.sum() == y.sum())

    def test_assert_on_columns(self):
        # these should pass, since columns 'A' in df1 and df2 have the same sum
        dc.assert_on_columns(self.df1['A'], self.df2['A'], lambda x, y: x.sum() == y.sum())

        # this should fail, since columns 'B' in df1 and df3 have different sums
        with self.assertRaises(AssertionError):
            dc.assert_on_columns(self.df1['B'], self.df3['B'], lambda x, y: x.sum() == y.sum())

    def test_assert_change_within_threshold(self):
        # this should pass, since the sums for 'A' and 'B' are the same in both DFs
        dc.assert_change_within_threshold(self.summary, 'Sum', 'A', 0)
        dc.assert_change_within_threshold(self.summary, 'Sum', 'B', 0)

        # change the Sum DF2 for 'B', this should now fail the threshold test
        self.summary.iat[1, 2] = 16

        with self.assertRaises(AssertionError):
            dc.assert_change_within_threshold(self.summary, 'Sum', 'B', 0.05)

if __name__ == '__main__':
    unittest.main()
