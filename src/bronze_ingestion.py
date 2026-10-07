"""Orchestration for OLTP-to-bronze ingestion."""

from dataclasses import dataclass
from pathlib import Path
import uuid

from pyspark.sql import DataFrame, SparkSession

from src.bronze_audit import (
    IngestionAuditRecord,
    append_audit_record,
    complete_ingestion_audit,
    create_ingestion_audit_record,
    mark_ingestion_failed,
    mark_ingestion_succeeded,
    record_validation_metrics,
    record_write_metrics,
)

from src.bronze_metadata import add_metadata_columns
from src.bronze_source import load_entity_data, resolve_source_path
from src.bronze_validation import apply_validation_policy, validate_bronze_data
from src.bronze_writer import write_bronze_data


@dataclass(frozen=True)
class BronzeIngestionConfig:
    data_root: Path
    bronze_root: Path
    source_batch_id: str
    source_system: str


ENTITIES = [
    "address",
    "category",
    "customer",
    "order",
    "order_item",
    "payment",
    "product",
    "product_category",
    "return",
    "return_item",
]


def ingest_batch(
    spark: SparkSession,
    config: BronzeIngestionConfig,
    entities: list[str],
) -> tuple[IngestionAuditRecord, ...]:
    """Ingest selected entities using one run ID and return their audit results."""
    pipeline_run_id = str(uuid.uuid4())
    return tuple(
        ingest_entity(spark, config, entity, pipeline_run_id) for entity in entities
    )


def ingest_entity(
    spark: SparkSession,
    config: BronzeIngestionConfig,
    entity: str,
    pipeline_run_id: str,
) -> IngestionAuditRecord:
    """Coordinate one entity's ingestion lifecycle and audit persistence."""
    source_path = resolve_source_path(config.data_root, config.source_batch_id, entity)
    target = config.bronze_root / entity
    audit = create_ingestion_audit_record(
        source_batch_id=config.source_batch_id,
        pipeline_run_id=pipeline_run_id,
        source_system=config.source_system,
        entity=entity,
        source_file_name=source_path.name,
        source_file_path=str(source_path),
        target_table=str(target),
    )
    try:
        has_warnings = _ingest_entity_data(
            spark=spark,
            config=config,
            entity=entity,
            pipeline_run_id=pipeline_run_id,
            target=target,
            audit=audit,
        )
        mark_ingestion_succeeded(audit, has_warnings=has_warnings)
    except Exception as error:
        mark_ingestion_failed(audit, error)
        raise
    finally:
        complete_ingestion_audit(audit)
        append_audit_record(spark, audit, config.bronze_root / "ingestion_audit")

    return audit


def _ingest_entity_data(
    spark: SparkSession,
    config: BronzeIngestionConfig,
    entity: str,
    pipeline_run_id: str,
    target: Path,
    audit: IngestionAuditRecord,
) -> bool:
    """Run the load, validation, and write stages for one entity."""
    enriched_df = _load_and_enrich_entity_data(
        spark=spark,
        config=config,
        entity=entity,
        pipeline_run_id=pipeline_run_id,
        audit=audit,
    )
    has_warnings = _validate_entity_data(enriched_df, entity, audit)
    _write_entity_data(spark, enriched_df, target, entity, audit)
    return has_warnings


def _load_and_enrich_entity_data(
    spark: SparkSession,
    config: BronzeIngestionConfig,
    entity: str,
    pipeline_run_id: str,
    audit: IngestionAuditRecord,
) -> DataFrame:
    """Load one source entity and add its hash and ingestion metadata."""
    dataframe = load_entity_data(
        spark=spark,
        data_root=config.data_root,
        source_batch_id=config.source_batch_id,
        entity=entity,
    )
    return add_metadata_columns(
        df=dataframe,
        source_batch_id=config.source_batch_id,
        pipeline_run_id=pipeline_run_id,
        source_system=config.source_system,
    )


def _validate_entity_data(
    dataframe: DataFrame,
    entity: str,
    audit: IngestionAuditRecord,
) -> bool:
    """Validate an enriched entity and record its validation metrics."""
    report = validate_bronze_data(dataframe, entity)
    record_validation_metrics(audit, report.metrics)
    apply_validation_policy(report)
    return bool(report.metrics.corrupt_record_count)


def _write_entity_data(
    spark: SparkSession,
    dataframe: DataFrame,
    target: Path,
    entity: str,
    audit: IngestionAuditRecord,
) -> None:
    """Write validated entity data and record Delta write metrics."""
    metrics = write_bronze_data(spark=spark, df=dataframe, target=target)
    record_write_metrics(audit, metrics)
    if metrics.hash_conflict_count:
        raise ValueError(
            f"Found {metrics.hash_conflict_count} hash conflicts for "
            f"entity '{entity}'"
        )
