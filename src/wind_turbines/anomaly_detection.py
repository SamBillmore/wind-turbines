import pyspark.sql.functions as F
from pyspark.sql import DataFrame


def flag_anomalies(df: DataFrame) -> DataFrame:
    """
    Flag anomalies in the power output of wind turbines.
    Anomalies are defined as values that are more than 2 standard deviations away from
    the mean.
    :param df: DataFrame containing wind turbine data.
    :return: DataFrame with anomalies flagged.
    """
    # Calculate mean and standard deviation for each turbine
    stats = df.groupBy("turbine_id").agg(
        F.mean("power_output").alias("mean_power"),
        F.stddev("power_output").alias("stddev_power"),
    )
    # Join stats back to original data
    df = df.join(stats, on="turbine_id", how="left")

    # Add columns for upper and lower bounds
    df = df.withColumn("upper_bound", F.col("mean_power") + (2 * F.col("stddev_power")))
    df = df.withColumn("lower_bound", F.col("mean_power") - (2 * F.col("stddev_power")))

    # Flag anomalies
    return df.withColumn(
        "is_anomaly",
        (F.col("power_output") > F.col("upper_bound"))
        | (F.col("power_output") < F.col("lower_bound")),
    )
