from wind_turbines.anomaly_detection import flag_anomalies
from wind_turbines.cleaning import clean_raw_data
from wind_turbines.logger import get_logger
from wind_turbines.spark_utils import get_spark
from wind_turbines.storage_utils import csv_reader, table_reader, table_writer
from wind_turbines.summary_statistics import summary_statistics

logger = get_logger(__name__)


def ingest_data() -> None:
    """
    Ingest raw CSV data to delta table format.
    """
    raw_data_path = "/Volumes/wind_turbines/wind_turbines_raw/wind_turbines_raw/*.csv"
    raw_data_schema = (
        "timestamp TIMESTAMP, turbine_id INT, wind_speed DOUBLE, "
        "wind_direction INT, power_output DOUBLE"
    )
    spark_session = get_spark()
    df = csv_reader(
        spark=spark_session,
        path=raw_data_path,
        header=True,
        data_schema=raw_data_schema,
    )
    table_writer(
        df=df,
        catalog_name="wind_turbines",
        schema_name="wind_turbines_raw",
        table_name="wind_turbines_raw",
        mode="upsert",
        merge_condition=(
            "existing.timestamp = new.timestamp AND "
            "existing.turbine_id = new.turbine_id"
        ),
    )


def clean_data() -> None:
    """
    Clean the raw data and write it to a delta table.
    """
    spark_session = get_spark()
    df = table_reader(
        spark=spark_session,
        catalog_name="wind_turbines",
        schema_name="wind_turbines_raw",
        table_name="wind_turbines_raw",
    )
    df = clean_raw_data(df=df)
    table_writer(
        df=df,
        catalog_name="wind_turbines",
        schema_name="wind_turbines_clean",
        table_name="wind_turbines_clean",
        mode="upsert",
        merge_condition=(
            "existing.timestamp = new.timestamp AND "
            "existing.turbine_id = new.turbine_id"
        ),
    )


def calculate_summary_statistics() -> None:
    """
    Calculate summary statistics for the cleaned data and write the results to a delta
    table.
    """
    spark_session = get_spark()
    df = table_reader(
        spark=spark_session,
        catalog_name="wind_turbines",
        schema_name="wind_turbines_clean",
        table_name="wind_turbines_clean",
    )
    df = summary_statistics(df=df)
    table_writer(
        df=df,
        catalog_name="wind_turbines",
        schema_name="wind_turbines_enriched",
        table_name="wind_turbines_summary_statistics",
        mode="overwrite",
    )


def identify_anomalies() -> None:
    """
    Identify anomalies in the cleaned data and write the results to a delta table.
    """
    spark_session = get_spark()
    df = table_reader(
        spark=spark_session,
        catalog_name="wind_turbines",
        schema_name="wind_turbines_clean",
        table_name="wind_turbines_clean",
    )
    df = flag_anomalies(df=df)
    table_writer(
        df=df,
        catalog_name="wind_turbines",
        schema_name="wind_turbines_enriched",
        table_name="wind_turbines_anomalies_identified",
        mode="overwrite",
    )
