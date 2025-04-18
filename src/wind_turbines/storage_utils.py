from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession

from wind_turbines.logger import get_logger

logger = get_logger(__name__)


def csv_reader(
    spark: SparkSession, path: str, header: bool, data_schema: str
) -> DataFrame:
    """
    Read a CSV file into a DataFrame.
    :param spark: Spark session.
    :param path: Path to the CSV file.
    :param header: Whether the CSV file has a header.
    :return: DataFrame.
    """
    logger.info(f"Reading CSV file from {path}")
    if not path.endswith(".csv"):
        raise ValueError("Path to CSV file must end with '.csv'.")
    return spark.read.csv(path=path, header=header, schema=data_schema)


def table_reader(
    spark: SparkSession, catalog_name: str, schema_name: str, table_name: str
) -> DataFrame:
    """
    Read a Databricks table into a DataFrame.
    :param spark: Spark session.
    :param catalog_name: Name of the catalog.
    :param schema_name: Name of the schema.
    :param table_name: Name of the table.
    :return: DataFrame.
    """
    table_location = f"{catalog_name}.{schema_name}.{table_name}"
    logger.info(f"Reading Databricks table from {table_location}")
    return spark.read.table(tableName=table_location)


def table_writer(
    df: DataFrame,
    catalog_name: str,
    schema_name: str,
    table_name: str,
    mode: str = "upsert",
    merge_condition: str | None = None,
) -> None:
    """
    Write a Dataframe to a Databricks table.
    :param df: DataFrame to write.
    :param catalog_name: Name of the catalog.
    :param schema_name: Name of the schema.
    :param table_name: Name of the table.
    :param mode: Write mode ('upsert' or 'overwrite').
    :param merge_condition: Merge condition for UPSERT mode.
    :raises ValueError: If the mode is not supported or if merge_condition is not
        provided for UPSERT mode.
    :return: DataFrame.
    """
    table_location = f"{catalog_name}.{schema_name}.{table_name}"
    if not df.sparkSession.catalog.tableExists(tableName=table_location):
        raise ValueError(
            f"Please create the required table {table_location} using Terraform."
        )
    if mode == "overwrite":
        _overwrite_table_writer(df=df, table_location=table_location)
        return
    if mode == "upsert":
        _upsert_table_writer(
            df=df, table_location=table_location, merge_condition=merge_condition
        )
        return
    raise ValueError(
        f"Unsupported mode '{mode}'. Supported modes are 'upsert' and 'overwrite'."
    )


def _overwrite_table_writer(
    df: DataFrame,
    table_location: str,
) -> None:
    df.write.mode("overwrite").saveAsTable(name=table_location)


def _upsert_table_writer(
    df: DataFrame,
    table_location: str,
    merge_condition: str | None = None,
) -> None:
    if merge_condition is None:
        raise ValueError("Merge condition must be provided for UPSERT mode.")
    existing = DeltaTable.forName(
        sparkSession=df.sparkSession, tableOrViewName=table_location
    )
    existing.alias("existing").merge(
        source=df.alias("new"),
        condition=merge_condition,
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
