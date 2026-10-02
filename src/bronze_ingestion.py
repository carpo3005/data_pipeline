"""Orchestration for OLTP-to-bronze ingestion."""

from dataclasses import dataclass
from pathlib import Path
import uuid

from pyspark.sql import SparkSession

from data_pipeline.src.bronze_metadata import add_metadata_columns
from data_pipeline.src.bronze_source import load_entity_data
from data_pipeline.src.bronze_validation import validate_bronze_data
from data_pipeline.src.bronze_writer import write_bronze_data


@dataclass(frozen=True)
class BronzeIngestionConfig:
    data_root: Path
    bronze_root: Path
    batch_id: str
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
    run_id = str(uuid.uuid4())
    for entity in entities:
        ingest_entity(spark, config, entity, run_id)


def ingest_entity(
    spark: SparkSession,
    config: BronzeIngestionConfig,
    entity: str,
    run_id: str,
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
        run_id=run_id,
        source_system=config.source_system,
    )
    validate_bronze_data(df, entity)
    write_bronze_data(
        spark=spark,
        df=df,
        target=config.bronze_root / entity,
    )
