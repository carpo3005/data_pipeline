"""Delta table creation and idempotent-write operations."""

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession


def prepare_merge_source(df: DataFrame, merge_key: str) -> DataFrame:
    """Prepare incoming rows according to the selected merge-key policy."""
    raise NotImplementedError


def create_delta_table(
    spark: SparkSession,
    df: DataFrame,
    target: Path,
) -> None:
    """Create the Delta table for its first write."""
    raise NotImplementedError


def merge_new_records(
    spark: SparkSession,
    df: DataFrame,
    target: Path,
    merge_key: str,
) -> None:
    """Insert rows not already represented in the Delta target."""
    raise NotImplementedError


def write_bronze_data(
    spark: SparkSession,
    df: DataFrame,
    target: Path,
    merge_key: str = "_record_hash",
) -> None:
    """Create a Delta target or merge incoming rows into an existing one."""
    raise NotImplementedError
