"""Audit records and persistence for bronze ingestion runs."""

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from delta.tables import DeltaTable

from src.bronze_validation import ValidationMetrics
from src.bronze_writer import WriteMetrics


@dataclass
class IngestionAuditRecord:
    source_batch_id: str
    pipeline_run_id: str
    source_system: str
    entity: str
    source_file_name: str
    source_file_path: str
    target_table: str
    source_row_count: int | None
    corrupt_record_count: int | None
    null_business_key_count: int | None
    duplicate_business_key_count: int | None
    duplicate_rows_beyond_first: int | None
    replay_existing_row_count: int | None
    inserted_row_count: int | None
    hash_conflict_count: int | None
    target_total_row_count: int | None
    status: str
    error_message: str | None
    started_at: datetime
    completed_at: datetime


AUDIT_SCHEMA = StructType(
    [
        StructField("source_batch_id", StringType(), nullable=False),
        StructField("pipeline_run_id", StringType(), nullable=False),
        StructField("source_system", StringType(), nullable=False),
        StructField("entity", StringType(), nullable=False),
        StructField("source_file_name", StringType(), nullable=False),
        StructField("source_file_path", StringType(), nullable=False),
        StructField("target_table", StringType(), nullable=False),
        StructField("source_row_count", LongType(), nullable=True),
        StructField("corrupt_record_count", LongType(), nullable=True),
        StructField("null_business_key_count", LongType(), nullable=True),
        StructField("duplicate_business_key_count", LongType(), nullable=True),
        StructField("duplicate_rows_beyond_first", LongType(), nullable=True),
        StructField("replay_existing_row_count", LongType(), nullable=True),
        StructField("inserted_row_count", LongType(), nullable=True),
        StructField("hash_conflict_count", LongType(), nullable=True),
        StructField("target_total_row_count", LongType(), nullable=True),
        StructField("status", StringType(), nullable=False),
        StructField("error_message", StringType(), nullable=True),
        StructField("started_at", TimestampType(), nullable=False),
        StructField("completed_at", TimestampType(), nullable=False),
    ]
)


def create_ingestion_audit_record(
    source_batch_id: str,
    pipeline_run_id: str,
    source_system: str,
    entity: str,
    source_file_name: str,
    source_file_path: str,
    target_table: str,
) -> IngestionAuditRecord:
    """Create an in-progress audit record with known ingestion identifiers."""
    now = datetime.now(timezone.utc)
    return IngestionAuditRecord(
        source_batch_id=source_batch_id,
        pipeline_run_id=pipeline_run_id,
        source_system=source_system,
        entity=entity,
        source_file_name=source_file_name,
        source_file_path=source_file_path,
        target_table=target_table,
        source_row_count=None,
        corrupt_record_count=None,
        null_business_key_count=None,
        duplicate_business_key_count=None,
        duplicate_rows_beyond_first=None,
        replay_existing_row_count=None,
        inserted_row_count=None,
        hash_conflict_count=None,
        target_total_row_count=None,
        status="IN_PROGRESS",
        error_message=None,
        started_at=now,
        completed_at=now,
    )


def record_validation_metrics(
    record: IngestionAuditRecord,
    metrics: ValidationMetrics,
) -> None:
    """Copy validation counts into their corresponding audit fields."""
    record.source_row_count = metrics.row_count
    record.corrupt_record_count = metrics.corrupt_record_count
    record.null_business_key_count = (
        metrics.null_or_blank_source_record_id_count
    )
    record.duplicate_business_key_count = (
        metrics.distinct_duplicated_source_record_id_count
    )
    record.duplicate_rows_beyond_first = (
        metrics.duplicate_rows_beyond_first_count
    )


def record_write_metrics(
    record: IngestionAuditRecord,
    metrics: WriteMetrics,
) -> None:
    """Copy Delta write counts into their corresponding audit fields."""
    record.replay_existing_row_count = metrics.replay_existing_row_count
    record.inserted_row_count = metrics.inserted_row_count
    record.hash_conflict_count = metrics.hash_conflict_count
    record.target_total_row_count = metrics.target_total_row_count


def mark_ingestion_succeeded(
    record: IngestionAuditRecord,
    *,
    has_warnings: bool,
) -> None:
    """Set the final success status for an entity ingestion."""
    record.status = "SUCCESS_WITH_WARNINGS" if has_warnings else "SUCCESS"


def mark_ingestion_failed(record: IngestionAuditRecord, error: Exception) -> None:
    """Set the failure status and message for an entity ingestion."""
    record.status = "FAILED"
    record.error_message = f"{type(error).__name__}: {error}"


def complete_ingestion_audit(record: IngestionAuditRecord) -> None:
    """Set the completion timestamp after all entity-ingestion stages finish."""
    record.completed_at = datetime.now(timezone.utc)


def append_audit_record(
    spark: SparkSession,
    record: IngestionAuditRecord,
    audit_target: Path,
) -> None:
    """Append one completed entity-ingestion audit record to Delta."""
    audit_df = spark.createDataFrame([asdict(record)], schema=AUDIT_SCHEMA)

    if DeltaTable.isDeltaTable(spark, str(audit_target)):
        audit_df.write.format("delta").mode("append").save(str(audit_target))
    elif audit_target.exists():
        raise ValueError(
            f"Audit path exists but is not a Delta table: {audit_target}"
        )
    else:
        audit_df.write.format("delta").mode("errorifexists").save(str(audit_target))
