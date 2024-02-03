from pyspark.sql import functions as F
from pyspark.sql.types import ByteType, DecimalType, DoubleType, FloatType, IntegerType, LongType, ShortType

def is_numeric(data_type):
    return isinstance(data_type, (ByteType, DecimalType, DoubleType, FloatType, IntegerType, LongType, ShortType))

def _calculate_outliers(df, col):
    quantiles = df.approxQuantile(col, [0.25, 0.75], 0)
    IQR = quantiles[1] - quantiles[0]

    lower_range = quantiles[0] - 1.5 * IQR
    upper_range = quantiles[1] + 1.5 * IQR

    # Count of outliers
    outlier_count = df.filter((df[col] < lower_range) | (df[col] > upper_range)).count()

    # Gather a sample of outliers
    outlier_values = df.filter((df[col] < lower_range) | (df[col] > upper_range)).select(col).limit(20).rdd.flatMap(lambda x: x).collect()

    return {"count": outlier_count, "lower_range": lower_range, "upper_range": upper_range, "values": outlier_values}

def _compare_dataframes(df1, df2):
    column_diffs = []

    for col in df1.columns:
        # Number and percent of differences per column
        col_diff = df1.filter(df1[col] != df2[col]).count()
        per_diff = col_diff / df1.count() * 100

        # number of non-null rows and distinct values in both dataframes
        non_null_rows_df1 = df1.filter(df1[col].isNotNull()).count()
        non_null_rows_df2 = df2.filter(df2[col].isNotNull()).count()

        distinct_values_df1 = df1.agg(F.countDistinct(df1[col])).first()[0]
        distinct_values_df2 = df2.agg(F.countDistinct(df2[col])).first()[0]

        # top 5 values for each dataframes
        top_values_df1 = df1.groupBy(col).count().orderBy(F.col("count").desc()).limit(5).collect()
        top_values_df2 = df2.groupBy(col).count().orderBy(F.col("count").desc()).limit(5).collect()

        # If the column type is numeric
        if is_numeric(df1.schema[col].dataType):
            stats_df1 = df1.select(
                F.mean(col).alias('mean'),
                F.stddev(col).alias('stddev'),
                F.sum(col).alias('sum')
            ).first()

            stats_df2 = df2.select(
                F.mean(col).alias('mean'),
                F.stddev(col).alias('stddev'),
                F.sum(col).alias('sum')
            ).first()

            # Calculate approximate median. Set the relative error parameter to 0.01.
            median_df1 = df1.approxQuantile(col, [0.5], 0.01)[0]
            median_df2 = df2.approxQuantile(col, [0.5], 0.01)[0]

            # Calculate outliers
            outliers_df1 = _calculate_outliers(df1, col)
            outliers_df2 = _calculate_outliers(df2, col)

        else:
            stats_df1 = stats_df2 = median_df1 = median_df2 = outliers_df1 = outliers_df2 = None

        column_diffs.append([col, col_diff, per_diff, non_null_rows_df1, non_null_rows_df2,
                             distinct_values_df1, distinct_values_df2, top_values_df1, top_values_df2,
                             stats_df1, stats_df2, median_df1, median_df2, outliers_df1, outliers_df2])

    return column_diffs
