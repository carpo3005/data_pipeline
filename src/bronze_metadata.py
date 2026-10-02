"""Raw-payload hashing and ingestion metadata enrichment."""

from pyspark.sql import DataFrame


def add_record_hash(df: DataFrame) -> DataFrame:
    """Add a deterministic hash of the raw source columns."""
    raise NotImplementedError


def add_ingestion_metadata(
    df: DataFrame,
    batch_id: str,
    run_id: str,
    source_system: str,
) -> DataFrame:
    """Add batch, run, source-system, and ingestion-time columns."""
    raise NotImplementedError


def add_metadata_columns(
    df: DataFrame,
    batch_id: str,
    run_id: str,
    source_system: str,
) -> DataFrame:
    """Add the record hash and ingestion metadata to the source DataFrame."""
    raise NotImplementedError
