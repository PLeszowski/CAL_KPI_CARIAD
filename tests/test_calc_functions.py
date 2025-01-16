import unittest
import calc_functions as cf
import pandas as pd

df1 = pd.DataFrame({'value': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 'weight': [1, 1, 1, 1, 1, 1, 1, 1, 1, 1]})

df2 = pd.DataFrame({'value': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 'weight': [0, 1, 1, 1, 1, 1, 1, 1, 1, 1]})
s2 = pd.Series([2, 3, 4, 5, 6, 7, 8, 9, 10])

df3 = pd.DataFrame({'value': [55, 75, 34, 53, 65, 98, 7, 21, 45, 10], 'weight': [3, 8, 3, 6, 5, 8, 3, 4, 4, 1]})
s3 = pd.Series([55, 55, 55,
                75, 75, 75, 75, 75, 75, 75, 75,
                34, 34, 34,
                53, 53, 53, 53, 53, 53,
                65, 65, 65, 65, 65,
                98, 98, 98, 98, 98, 98, 98, 98,
                7, 7, 7,
                21, 21, 21, 21,
                45, 45, 45, 45,
                10])

values = list(range(1, 101))
weights = [1] * 100

df4 = pd.DataFrame({'value': values, 'weight': weights})


class TestWeighedQuantile(unittest.TestCase):
    """
    Test calc_functions.weighed_percentile against pandas built in quantile
    """

    def test_weighed_quantile_df1(self):
        val = -1
        for i in range(0, 101):
            try:
                val = i / 100
                expected = round(df1['value'].quantile(val), 3)
                result = round(cf.weighed_percentile(df1, val), 3)
                self.assertEqual(expected, result)
            except AssertionError as e:
                print(f'expected != result at val={val}')
                raise e

    def test_weighed_quantile_df2(self):
        val = -1
        for i in range(0, 101):
            try:
                val = i / 100
                expected = round(s2.quantile(val), 3)
                result = round(cf.weighed_percentile(df2, val), 3)
                self.assertEqual(expected, result)
            except AssertionError as e:
                print(f'expected != result at val={val}')
                raise e

    def test_weighed_quantile_df3(self):
        for i in range(0, 101):
            val = -1
            try:
                val = i / 100
                expected = round(s3.quantile(val), 3)
                result = round(cf.weighed_percentile(df3, val), 3)
                self.assertEqual(expected, result)
            except AssertionError as e:
                print(f'expected != result at val={val}')
                raise e

    def test_calc_weighed_percentile_df_df1(self):
        expected = cf.calc_percentile_df(df1, 'value')
        result = cf.calc_weighed_percentile_df(df1, 'value')
        self.assertEqual(expected, result)

    def test_calc_weighed_percentile_df_df4(self):
        expected = cf.calc_percentile_df(df4, 'value')
        result = cf.calc_weighed_percentile_df(df4, 'value')
        self.assertEqual(expected, result)

    def test_calc_weighed_percentile_key_error(self):
        self.assertRaises(KeyError)


if __name__ == '__main__':
    unittest.main()
