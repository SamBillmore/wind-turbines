import atexit
import logging
from typing import Generator
from unittest.mock import MagicMock

import pytest
from pyspark.sql import SparkSession

# Configure logging with a NullHandler to avoid errors during shutdown
logging.getLogger("py4j").addHandler(logging.NullHandler())
logger = logging.getLogger(__name__)


@pytest.fixture(scope="session")
def spark() -> Generator[SparkSession, None, None]:
    """Configure and create the Spark session for all tests.

    A temporary directory will be created and used to store all Delta tables.

    :param tmp_path_factory: pytest's TmpPathFactory fixture,
    see https://docs.pytest.org/en/stable/how-to/tmp_path.html
    :return: the Spark session for all unit tests
    """
    logger.info("Initializing Spark session for testing")

    # Build and configure spark session
    spark = (
        SparkSession.builder.master("local[1]")
        .appName("unit-tests")
        .config("spark.executor.cores", "1")
        .config("spark.executor.instances", "1")
        .config("spark.driver.memory", "2g")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.default.parallelism", "1")
        .config("spark.sql.adaptive.enabled", "true")
        # Disabling rdd and map output compression as data is already small for tests
        .config("spark.rdd.compress", False)
        .config("spark.shuffle.compress", False)
        # Disable Spark UI for tests
        .config("spark.ui.enabled", False)
        .config("spark.ui.showConsoleProgress", False)
        .config("spark.executor.extraJavaOptions", "-Dlog4j.logLevel=ERROR")
        .config("spark.driver.extraJavaOptions", "-Dlog4j.logLevel=ERROR")
        .getOrCreate()
    )

    # Set log level for testing
    spark.sparkContext.setLogLevel("ERROR")

    # Register cleanup function
    def cleanup():
        try:
            if spark is not None:
                spark.stop()
        except Exception:
            pass

    # Register cleanup to run at exit
    atexit.register(cleanup)

    yield spark

    # Cleanup after tests
    cleanup()


@pytest.fixture(scope="session")
def spark_databricks_mock() -> MagicMock:
    """Fixture for mocking spark interactions with Databricks."""
    return MagicMock()
