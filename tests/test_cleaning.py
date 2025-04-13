from datetime import datetime

from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql.session import SparkSession

from wind_turbines.cleaning import clean_raw_data


def test_clean_raw_data(spark: SparkSession) -> None:
    """
    Test the clean_raw_data function.
    """
    # Given some input data
    input_data = [
        ("2023-01-01 00:00:00", 1, 11.6, 170, 10.0),
        ("2023-01-01 01:00:00", 1, 10.1, 156, 12.0),
        ("2023-01-01 01:00:00", 1, 10.1, 156, 12.0),  # Duplicate
        ("2023-01-01 02:00:00", 2, 9.4, 140, -5.0),  # Invalid power output
        ("2023-01-01 03:00:00", 2, None, None, None),  # Missing value
        ("2023-01-01 04:00:00", 1, 12.5, 90, 15.0),
    ]
    input_schema = (
        "timestamp STRING, turbine_id INT, wind_speed DOUBLE, "
        "wind_direction INTEGER, power_output DOUBLE"
    )
    input_df = spark.createDataFrame(input_data, schema=input_schema)

    # When we call the function
    actual = clean_raw_data(df=input_df)

    # Then the result is as expected
    expected_data = [
        (datetime(2023, 1, 1, 0, 0, 0), 1, 11.6, 170, 10.0),
        (datetime(2023, 1, 1, 1, 0, 0), 1, 10.1, 156, 12.0),
        (datetime(2023, 1, 1, 4, 0, 0), 1, 12.5, 90, 15.0),
    ]
    expected_schema = (
        "timestamp TIMESTAMP, turbine_id INT, wind_speed DOUBLE, "
        "wind_direction INTEGER, power_output DOUBLE"
    )
    expected = spark.createDataFrame(expected_data, schema=expected_schema)
    assert_df_equality(actual, expected, ignore_nullable=False)
