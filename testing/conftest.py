import pytest

from src.spark_setup import get_spark_session

from pyspark.sql import DataFrame

@pytest.fixture(scope="session")
def spark():
    """Fixture to create a SparkSession for testing."""
    spark = get_spark_session(app_name = "test_session")
    yield spark
    spark.stop()

@pytest.fixture(scope="session")
def sample_df():
    sample_df = spark.createDataFrame(
        ["James", None, 30],
        ["James", None, 30], # 1 & 2 are repeated
        ["James", "Jones", 30],
    )