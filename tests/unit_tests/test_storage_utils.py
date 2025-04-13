from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from chispa.dataframe_comparer import assert_df_equality
from delta.tables import DeltaTable
from pyspark.sql.session import SparkSession

from wind_turbines.storage_utils import csv_reader, table_reader, table_writer


def test_csv_reader(spark: SparkSession, tmp_path: Path) -> None:
    """
    Test the csv_reader function.
    """
    # Given some input data
    dir = tmp_path / "data"
    dir.mkdir()
    input_file = str(dir / "input_data.csv")
    input_data = [
        ("2023-01-01 00:00:00", 1, 11.6, 170, 10.0),
        ("2023-01-01 01:00:00", 1, 10.1, 156, 12.0),
        ("2023-01-01 02:00:00", 2, 9.4, 140, -5.0),
        ("2023-01-01 03:00:00", 2, None, None, None),
        ("2023-01-01 04:00:00", 1, 12.5, 90, 15.0),
    ]
    df = pd.DataFrame(
        input_data,
        columns=[
            "timestamp",
            "turbine_id",
            "wind_speed",
            "wind_direction",
            "power_output",
        ],
    )
    df["wind_direction"] = df["wind_direction"].astype("Int32")
    df.to_csv(input_file, index=False)

    # When we call the function
    actual = csv_reader(
        spark=spark,
        path=input_file,
        header=True,
        data_schema=(
            "timestamp TIMESTAMP, turbine_id INT, wind_speed DOUBLE, "
            "wind_direction INTEGER, power_output DOUBLE"
        ),
    )

    # Then the result is as expected
    expected_data = [
        (
            datetime.strptime(row[0], "%Y-%m-%d %H:%M:%S"),
            row[1],
            row[2],
            row[3],
            row[4],
        )
        for row in input_data
    ]
    expected = spark.createDataFrame(
        expected_data,
        schema=(
            "timestamp TIMESTAMP, turbine_id INT, wind_speed DOUBLE, "
            "wind_direction INTEGER, power_output DOUBLE"
        ),
    )
    assert_df_equality(actual, expected, ignore_nullable=False)


def test_csv_reader_invalid_path(spark: SparkSession) -> None:
    """
    Test the csv_reader function with an invalid path.
    """
    # Given an invalid path
    invalid_path = "invalid_path.txt"

    # When we call the function
    with pytest.raises(ValueError, match="Path to CSV file must end with '.csv'."):
        csv_reader(
            spark=spark,
            path=invalid_path,
            header=True,
            data_schema=(
                "timestamp TIMESTAMP, turbine_id INT, wind_speed DOUBLE, "
                "wind_direction INTEGER, power_output DOUBLE"
            ),
        )


def test_table_reader(spark: SparkSession, spark_databricks_mock: MagicMock) -> None:
    """
    Test the table_reader function locally.
    """
    # Given some input data
    catalog_name = "test_catalog"
    schema_name = "test_schema"
    table_name = "test_table"

    # And given a mocked response from Databricks that returns a DataFrame
    mock_df = spark.createDataFrame([(1, "test"), (2, "data")], schema=["id", "value"])
    spark_databricks_mock.read.table.return_value = mock_df

    # When we call the function
    actual = table_reader(
        spark=spark_databricks_mock,
        catalog_name=catalog_name,
        schema_name=schema_name,
        table_name=table_name,
    )

    # Then the result is as expected
    assert_df_equality(actual, mock_df, ignore_nullable=False)


def test_table_writer_upsert(spark: SparkSession) -> None:
    """Test the table_writer function with upsert mode."""
    # Given some input data
    df = spark.createDataFrame([(1, "test")], schema=["id", "value"])

    # Mock the Delta table operations
    with patch.object(DeltaTable, "forName") as mock_delta:
        # Create mock objects
        mock_delta_table = MagicMock()
        mock_delta.return_value = mock_delta_table
        mock_delta_table.alias.return_value = mock_delta_table
        mock_delta_table.merge.return_value = mock_delta_table
        mock_delta_table.whenMatchedUpdateAll.return_value = mock_delta_table
        mock_delta_table.whenNotMatchedInsertAll.return_value = mock_delta_table

        # Set table exists to True
        df.sparkSession.catalog.tableExists = MagicMock(return_value=True)

        # When we call the function
        table_writer(
            df=df,
            catalog_name="test_catalog",
            schema_name="test_schema",
            table_name="test_table",
            mode="upsert",
            merge_condition="existing.id = new.id",
        )

        # Then verify the calls
        mock_delta.assert_called_once_with(
            sparkSession=spark, tableOrViewName="test_catalog.test_schema.test_table"
        )
        mock_delta_table.merge.assert_called_once()
        mock_delta_table.whenMatchedUpdateAll.assert_called_once()
        mock_delta_table.whenNotMatchedInsertAll.assert_called_once()
        mock_delta_table.execute.assert_called_once()


def test_table_writer_upsert_error(spark: SparkSession) -> None:
    """Test the table_writer function with UPSERT without a merge_condition."""
    # Given some input data
    df = spark.createDataFrame([(1, "test")], schema=["id", "value"])
    catalog_name = "test_catalog"
    schema_name = "test_schema"
    table_name = "test_table"
    mode = "upsert"
    merge_condition = None

    # Set table exists to True
    df.sparkSession.catalog.tableExists = MagicMock(return_value=True)

    # When we call the function with an invalid mode
    # Then the correct error is raised
    msg = "Merge condition must be provided for UPSERT mode."
    with pytest.raises(ValueError, match=msg):
        table_writer(
            df=df,
            catalog_name=catalog_name,
            schema_name=schema_name,
            table_name=table_name,
            mode=mode,
            merge_condition=merge_condition,
        )


def test_table_writer_overwrite(spark: SparkSession) -> None:
    """Test the table_writer function with overwrite mode."""
    # Given some input data
    df = spark.createDataFrame([(1, "test")], schema=["id", "value"])

    # Mock the DataFrame write operation
    with patch("pyspark.sql.DataFrame.write", new_callable=MagicMock) as mock_writer:
        # Set table exists to True
        df.sparkSession.catalog.tableExists = MagicMock(return_value=True)

        # When we call the function
        table_writer(
            df=df,
            catalog_name="test_catalog",
            schema_name="test_schema",
            table_name="test_table",
            mode="overwrite",
        )

        # Then verify the calls
        mock_writer.mode.assert_called_once_with("overwrite")
        mock_writer.mode.return_value.saveAsTable.assert_called_once_with(
            name="test_catalog.test_schema.test_table"
        )


def test_table_writer_error(spark: SparkSession) -> None:
    """Test the table_writer function with an invalid mode."""
    # Given some input data
    df = spark.createDataFrame([(1, "test")], schema=["id", "value"])
    catalog_name = "test_catalog"
    schema_name = "test_schema"
    table_name = "test_table"
    mode = "invalid_mode"

    # Set table exists to True
    df.sparkSession.catalog.tableExists = MagicMock(return_value=True)

    # When we call the function with an invalid mode
    # Then the correct error is raised
    msg = f"Unsupported mode '{mode}'. Supported modes are 'upsert' and 'overwrite'."
    with pytest.raises(ValueError, match=msg):
        table_writer(
            df=df,
            catalog_name=catalog_name,
            schema_name=schema_name,
            table_name=table_name,
            mode=mode,
        )
