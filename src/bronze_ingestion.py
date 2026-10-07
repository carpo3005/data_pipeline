"""Orchestration for OLTP-to-bronze ingestion."""

from dataclasses import dataclass
from pathlib import Path
import uuid

from pyspark.sql import SparkSession

from src.bronze_metadata import add_metadata_columns
from src.bronze_source import load_entity_data
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


def ingest_batch(spark: SparkSession, config: BronzeIngestionConfig, entities: list[str]) -> None:
    """Ingest the selected entities using one run ID for the whole batch."""
    pipeline_run_id = str(uuid.uuid4())
    for entity in entities:
        ingest_entity(spark, config, entity, pipeline_run_id)


def ingest_entity(
    spark: SparkSession,
    config: BronzeIngestionConfig,
    entity: str,
    pipeline_run_id: str,
) -> None:
    """Load, enrich, validate, and write one entity."""
    df = load_entity_data(
        spark=spark,
        data_root=config.data_root,
        batch_id=config.batch_id,
        entity=entity,
    )
    df = add_metadata_columns(
        df=df,
        batch_id=config.batch_id,
        pipeline_run_id=pipeline_run_id,
        source_system=config.source_system,
    )
    validation_report = validate_bronze_data(df, entity)
    apply_validation_policy(validation_report)
    write_bronze_data(
        spark=spark,
        df=df,
        target=config.bronze_root / entity,
    )
