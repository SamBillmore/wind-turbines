import pyspark.sql.functions as F
from pyspark.sql import DataFrame


def summary_statistics(df: DataFrame) -> DataFrame:
    """
    Calculate summary statistics for wind turbine data.
    :param df: DataFrame containing wind turbine data.
    :return: DataFrame with summary statistics.
    """
    # Convert timestamp to date for daily aggregation
    df = df.withColumn("date", df.timestamp.cast("date"))

    # Group by turbine_id and date, then calculate statistics
    return df.groupBy("turbine_id", "date").agg(
        F.min("power_output").alias("min_power"),
        F.max("power_output").alias("max_power"),
        F.avg("power_output").alias("avg_power"),
    )
