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
        (datetime(2023, 1, 1, 0, 0, 0), 1, 10.0),
        (datetime(2023, 1, 1, 1, 0, 0), 1, 12.0),
        (datetime(2023, 1, 1, 2, 0, 0), 1, 1.0),  # Anomaly
        (datetime(2023, 1, 1, 3, 0, 0), 1, 11.0),
        (datetime(2023, 1, 1, 4, 0, 0), 1, 15.0),
    ]
    input_schema = "timestamp STRING, turbine_id INT, power_output DOUBLE"
    input_df = spark.createDataFrame(input_data, schema=input_schema)

    # When we call the function
    actual = flag_anomalies(df=input_df)

    # Then the result is as expected
    expected_data = [
        (datetime(2023, 1, 1, 0, 0, 0), 1, 10.0, False),
        (datetime(2023, 1, 1, 1, 0, 0), 1, 12.0, False),
        (datetime(2023, 1, 1, 2, 0, 0), 1, 1.0, True),  # Anomaly
        (datetime(2023, 1, 1, 3, 0, 0), 1, 11.0, False),
        (datetime(2023, 1, 1, 4, 0, 0), 1, 15.0, False),
    ]
    expected_schema = (
        "timestamp STRING, turbine_id INT, power_output DOUBLE, is_anomaly BOOLEAN"
    )
    expected = spark.createDataFrame(expected_data, schema=expected_schema)
    assert_df_equality(actual, expected, ignore_nullable=False)
