from datetime import datetime

from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql.session import SparkSession

from wind_turbines.summary_statistics import summary_statistics


def test_summary_statistics(spark: SparkSession) -> None:
    """
    Test the summary_statistics function.
    """
    # Given some input data
    input_data = [
        (datetime(2023, 1, 1, 0, 0, 0), 1, 11.6, 170, 10.0),
        (datetime(2023, 1, 1, 1, 0, 0), 1, 10.1, 156, 12.0),
        (datetime(2023, 1, 1, 4, 0, 0), 1, 12.5, 90, 14.0),
        (datetime(2023, 1, 1, 0, 0, 0), 2, 8.3, 170, 8.9),
        (datetime(2023, 1, 1, 1, 0, 0), 2, 8.1, 156, 8.8),
        (datetime(2023, 1, 1, 4, 0, 0), 2, 8.0, 90, 8.7),
    ]
    input_schema = (
        "timestamp TIMESTAMP, turbine_id INT, wind_speed DOUBLE, "
        "wind_direction INTEGER, power_output DOUBLE"
    )
    input_df = spark.createDataFrame(input_data, schema=input_schema)

    # When we call the function
    actual = summary_statistics(df=input_df)

    # Then the result is as expected
    expected_data = [
        (1, datetime(2023, 1, 1).date(), 10.0, 14.0, 12.0),
        (2, datetime(2023, 1, 1).date(), 8.7, 8.9, 8.8),
    ]
    expected_schema = (
        "turbine_id INT, date DATE, min_power DOUBLE, "
        "max_power DOUBLE, avg_power DOUBLE"
    )
    expected = spark.createDataFrame(expected_data, schema=expected_schema)
    assert_df_equality(actual, expected, ignore_nullable=False)
