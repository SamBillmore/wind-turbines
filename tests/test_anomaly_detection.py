from datetime import datetime

from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql.session import SparkSession

from wind_turbines.anomaly_detection import flag_anomalies


def test_flag_anomalies(spark: SparkSession) -> None:
    """
    Test the flag_anomalies function.
    """
    # Given some input data
    input_data = [
        (datetime(2023, 1, 1, 0, 0, 0), 1, 8.0),
        (datetime(2023, 1, 1, 1, 0, 0), 1, 8.0),
        (datetime(2023, 1, 1, 2, 0, 0), 1, 28.0),  # Anomaly
        (datetime(2023, 1, 1, 3, 0, 0), 1, 8.0),
        (datetime(2023, 1, 1, 4, 0, 0), 1, 8.0),
        (datetime(2023, 1, 1, 5, 0, 0), 1, 8.0),
        (datetime(2023, 1, 1, 6, 0, 0), 1, 8.0),
        (datetime(2023, 1, 1, 7, 0, 0), 1, 8.0),
        (datetime(2023, 1, 1, 8, 0, 0), 1, 8.0),
        (datetime(2023, 1, 1, 9, 0, 0), 1, 8.0),
    ]
    input_schema = "timestamp TIMESTAMP, turbine_id INT, power_output DOUBLE"
    input_df = spark.createDataFrame(input_data, schema=input_schema)

    # When we call the function
    actual = flag_anomalies(df=input_df)

    # Then the result is as expected
    expected_data = [
        (1, datetime(2023, 1, 1, 0, 0, 0), 8.0, 10.0, 6.3246, 22.6492, -2.6492, False),
        (1, datetime(2023, 1, 1, 1, 0, 0), 8.0, 10.0, 6.3246, 22.6492, -2.6492, False),
        (
            1,
            datetime(2023, 1, 1, 2, 0, 0),
            28.0,
            10.0,
            6.3246,
            22.6492,
            -2.6492,
            True,
        ),  # Anomaly
        (1, datetime(2023, 1, 1, 3, 0, 0), 8.0, 10.0, 6.3246, 22.6492, -2.6492, False),
        (1, datetime(2023, 1, 1, 4, 0, 0), 8.0, 10.0, 6.3246, 22.6492, -2.6492, False),
        (1, datetime(2023, 1, 1, 5, 0, 0), 8.0, 10.0, 6.3246, 22.6492, -2.6492, False),
        (1, datetime(2023, 1, 1, 6, 0, 0), 8.0, 10.0, 6.3246, 22.6492, -2.6492, False),
        (1, datetime(2023, 1, 1, 7, 0, 0), 8.0, 10.0, 6.3246, 22.6492, -2.6492, False),
        (1, datetime(2023, 1, 1, 8, 0, 0), 8.0, 10.0, 6.3246, 22.6492, -2.6492, False),
        (1, datetime(2023, 1, 1, 9, 0, 0), 8.0, 10.0, 6.3246, 22.6492, -2.6492, False),
    ]
    expected_schema = (
        "turbine_id INT, timestamp TIMESTAMP, power_output DOUBLE, "
        "mean_power DOUBLE, stddev_power DOUBLE, upper_bound DOUBLE, "
        "lower_bound DOUBLE, is_anomaly BOOLEAN"
    )
    expected = spark.createDataFrame(expected_data, schema=expected_schema)
    assert_df_equality(actual, expected, ignore_nullable=False)
