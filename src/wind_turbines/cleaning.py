import pyspark.sql.functions as F
from pyspark.sql import DataFrame


def clean_raw_data(df: DataFrame) -> DataFrame:
    """
    Clean raw wind turbine data.
    :param df: DataFrame containing raw wind turbine data.
    :return: Cleaned DataFrame.
    """
    # Remove duplicates
    df = df.dropDuplicates()

    # Remove rows with null values
    df = df.na.drop()

    # Convert timestamp to datetime
    df = df.withColumn("timestamp", F.to_timestamp(df.timestamp))

    # Filter out rows with negative power output
    df = df.filter(df.power_output >= 0)

    return df
