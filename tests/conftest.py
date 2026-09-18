import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """Create one Spark session for the entire test suite."""

    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("RetailPipelineTests")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )

    session.sparkContext.setLogLevel("ERROR")

    yield session

    session.stop()