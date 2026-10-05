import pytest
from pathlib import Path

from src.spark_setup import get_spark_session
from src.bronze_ingestion import BronzeIngestionConfig

# from pyspark.sql import DataFrame

@pytest.fixture(scope="session")
def spark():
    """Fixture to create a SparkSession for testing."""
    spark = get_spark_session(app_name = "test_session")
    yield spark
    spark.stop()

@pytest.fixture(scope="session")
def duplicate_rows_df(spark):
    return spark.createDataFrame(
        [
            ("James", "Jones", 30),
            ("James", "Jones", 30),
        ],
        ["first_name", "last_name", "age"],
    )

@pytest.fixture(scope="session")
def sample_df(spark):
    return spark.createDataFrame(
        [
            ("James", "Jones", 30),
            ("Ben", "Johnson", 12),
            ("Ellie", "Smith", 30),
            ("Anthony", "Naylor", 26),
            ("Dan", None, 30),
            # TODO: inject more edge cases here

        ],
        ["first_name", "last_name", "age"],
    )

@pytest.fixture(scope="session")
def diff_df(spark):
    return spark.createDataFrame(
        [
            ("James", "Jones", 30),
            ("Ben", "Johnson", 12),
        ],
        ["first_name", "last_name", "age"],
    )

@pytest.fixture(scope="session")
def ingestion_config():
    return BronzeIngestionConfig(
        data_root=Path("test_data_root"),
        bronze_root=Path("test_bronze_root"),
        source_batch_id="test_batch_001",
        source_system="test_system"
    )

@pytest.fixture(scope="session")
def pipeline_run_id():
    return "run-test-001"