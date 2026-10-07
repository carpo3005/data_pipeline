from pathlib import Path

import pytest

from pyspark.sql import SparkSession

from src.bronze_ingestion import BronzeIngestionConfig, ingest_entity
from src.bronze_audit import AUDIT_SCHEMA


def test_audit_schema_matches_expected_delta_table_contract() -> None:
    expected_schema = [
        ("source_batch_id", "string", False),
        ("pipeline_run_id", "string", False),
        ("source_system", "string", False),
        ("entity", "string", False),
        ("source_file_name", "string", False),
        ("source_file_path", "string", False),
        ("target_table", "string", False),
        ("source_row_count", "bigint", True),
        ("corrupt_record_count", "bigint", True),
        ("null_business_key_count", "bigint", True),
        ("duplicate_business_key_count", "bigint", True),
        ("duplicate_rows_beyond_first", "bigint", True),
        ("replay_existing_row_count", "bigint", True),
        ("inserted_row_count", "bigint", True),
        ("hash_conflict_count", "bigint", True),
        ("target_total_row_count", "bigint", True),
        ("status", "string", False),
        ("error_message", "string", True),
        ("started_at", "timestamp", False),
        ("completed_at", "timestamp", False),
    ]

    actual_schema = [
        (field.name, field.dataType.simpleString(), field.nullable)
        for field in AUDIT_SCHEMA.fields
    ]
    assert actual_schema == expected_schema


def _write_customer_csv(data_root: Path, batch_id: str, records: list[str]) -> Path:
    batch_dir = data_root / batch_id
    batch_dir.mkdir(parents=True)
    path = batch_dir / f"customer_{batch_id}.csv"
    path.write_text(
        "source_record_id,customer_id,first_name,last_name,email,phone,"
        "date_of_birth,loyalty_status,marketing_opt_in,created_at,updated_at\n"
        + "\n".join(records)
        + "\n",
        encoding="utf-8",
    )
    return path


def test_ingest_entity_returns_and_persists_audit_result(
    spark: SparkSession,
    tmp_path: Path,
) -> None:
    source_batch_id = "batch-001"
    source_path = _write_customer_csv(
        tmp_path / "source",
        source_batch_id,
        [
            "source-1,customer-1,Ada,Lovelace,ada@example.com,555-0101,"
            "1815-12-10,gold,true,2025-01-01,2025-01-02"
        ],
    )
    config = BronzeIngestionConfig(
        data_root=tmp_path / "source",
        bronze_root=tmp_path / "bronze",
        source_batch_id=source_batch_id,
        source_system="crm",
    )

    audit = ingest_entity(spark, config, "customer", "run-001")

    assert audit.source_batch_id == source_batch_id
    assert audit.pipeline_run_id == "run-001"
    assert audit.source_system == "crm"
    assert audit.entity == "customer"
    assert audit.source_file_name == source_path.name
    assert audit.source_file_path == str(source_path)
    assert audit.source_row_count == 1
    assert audit.corrupt_record_count == 0
    assert audit.null_business_key_count == 0
    assert audit.duplicate_business_key_count == 0
    assert audit.duplicate_rows_beyond_first == 0
    assert audit.replay_existing_row_count == 0
    assert audit.inserted_row_count == 1
    assert audit.hash_conflict_count == 0
    assert audit.target_total_row_count == 1
    assert audit.status == "SUCCESS"
    assert audit.started_at <= audit.completed_at

    audit_path = config.bronze_root / "ingestion_audit"
    persisted = spark.read.format("delta").load(str(audit_path))
    assert persisted.schema.names == AUDIT_SCHEMA.names
    assert persisted.count() == 1
    persisted_row = persisted.first()
    assert persisted_row["entity"] == "customer"
    assert persisted_row["inserted_row_count"] == 1
    assert persisted_row["status"] == "SUCCESS"


def test_ingest_entity_audits_validation_failure_before_reraising(
    spark: SparkSession,
    tmp_path: Path,
) -> None:
    source_batch_id = "batch-duplicate"
    _write_customer_csv(
        tmp_path / "source",
        source_batch_id,
        [
            "source-1,customer-1,Ada,Lovelace,ada@example.com,555-0101,"
            "1815-12-10,gold,true,2025-01-01,2025-01-02",
            "source-1,customer-2,Augusta,King,augusta@example.com,555-0102,"
            "1815-12-10,silver,false,2025-01-03,2025-01-04",
        ],
    )
    config = BronzeIngestionConfig(
        data_root=tmp_path / "source",
        bronze_root=tmp_path / "bronze",
        source_batch_id=source_batch_id,
        source_system="crm",
    )

    with pytest.raises(ValueError, match="duplicated source_record_id"):
        ingest_entity(spark, config, "customer", "run-duplicate")

    audit_path = config.bronze_root / "ingestion_audit"
    persisted = spark.read.format("delta").load(str(audit_path))
    row = persisted.first()
    assert row["status"] == "FAILED"
    assert row["source_row_count"] == 2
    assert row["duplicate_business_key_count"] == 1
    assert row["duplicate_rows_beyond_first"] == 1
    assert row["error_message"] is not None
    assert not (config.bronze_root / "customer").exists()


def test_ingest_entity_audits_replay_counts(
    spark: SparkSession,
    tmp_path: Path,
) -> None:
    source_batch_id = "batch-replay"
    _write_customer_csv(
        tmp_path / "source",
        source_batch_id,
        [
            "source-1,customer-1,Ada,Lovelace,ada@example.com,555-0101,"
            "1815-12-10,gold,true,2025-01-01,2025-01-02"
        ],
    )
    config = BronzeIngestionConfig(
        data_root=tmp_path / "source",
        bronze_root=tmp_path / "bronze",
        source_batch_id=source_batch_id,
        source_system="crm",
    )

    ingest_entity(spark, config, "customer", "run-first")
    replay_audit = ingest_entity(spark, config, "customer", "run-replay")

    assert replay_audit.status == "SUCCESS"
    assert replay_audit.replay_existing_row_count == 1
    assert replay_audit.inserted_row_count == 0
    assert replay_audit.target_total_row_count == 1


def test_ingest_entity_audits_hash_conflict_and_fails_without_mutating_target(
    spark: SparkSession,
    tmp_path: Path,
) -> None:
    data_root = tmp_path / "source"
    bronze_root = tmp_path / "bronze"
    first_batch = "batch-original"
    second_batch = "batch-changed"
    customer_record = (
        "source-1,customer-1,Ada,Lovelace,ada@example.com,555-0101,"
        "1815-12-10,gold,true,2025-01-01,2025-01-02"
    )
    _write_customer_csv(data_root, first_batch, [customer_record])
    changed_record = customer_record.replace("Ada,Lovelace", "Augusta,King")
    _write_customer_csv(data_root, second_batch, [changed_record])

    first_config = BronzeIngestionConfig(
        data_root=data_root,
        bronze_root=bronze_root,
        source_batch_id=first_batch,
        source_system="crm",
    )
    second_config = BronzeIngestionConfig(
        data_root=data_root,
        bronze_root=bronze_root,
        source_batch_id=second_batch,
        source_system="crm",
    )
    ingest_entity(spark, first_config, "customer", "run-original")

    with pytest.raises(ValueError, match="hash conflicts"):
        ingest_entity(spark, second_config, "customer", "run-changed")

    audit = (
        spark.read.format("delta")
        .load(str(bronze_root / "ingestion_audit"))
        .filter("source_batch_id = 'batch-changed'")
        .first()
    )
    assert audit["status"] == "FAILED"
    assert audit["hash_conflict_count"] == 1
    assert audit["inserted_row_count"] == 0
    assert audit["target_total_row_count"] == 1

    target = spark.read.format("delta").load(str(bronze_root / "customer"))
    assert target.count() == 1
    assert target.first()["first_name"] == "Ada"