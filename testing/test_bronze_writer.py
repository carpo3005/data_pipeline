from pathlib import Path

import pytest

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StringType, StructField, StructType

from delta.tables import DeltaTable

from src.bronze_writer import (
    create_delta_table,
    merge_new_records,
    validate_merge_keys,
    write_bronze_data,
)


SOURCE_SCHEMA = StructType(
    [
        StructField("_source_system", StringType(), nullable=False),
        StructField("source_record_id", StringType(), nullable=False),
        StructField("payload", StringType(), nullable=True),
    ]
)
COMPOSITE_KEYS = ("_source_system", "source_record_id")


@pytest.fixture
def bronze_rows_df(spark: SparkSession) -> DataFrame:
    return spark.createDataFrame(
        [
            ("erp_a", "id_1", "first"),
            ("erp_a", "id_2", "second"),
        ],
        schema=SOURCE_SCHEMA,
    )


def _rows(spark: SparkSession, rows: list[tuple[str, str, str]]) -> DataFrame:
    return spark.createDataFrame(rows, schema=SOURCE_SCHEMA)


@pytest.mark.parametrize(
    "merge_keys",
    [
        ("source_record_id",),
        COMPOSITE_KEYS,
    ],
    ids=["single-key", "composite-key"],
)
def test_validate_merge_keys_accepts_present_keys(
    bronze_rows_df: DataFrame,
    merge_keys: tuple[str, ...],
) -> None:
    validate_merge_keys(bronze_rows_df, merge_keys)


def test_validate_merge_keys_rejects_empty_keys(bronze_rows_df: DataFrame) -> None:
    with pytest.raises(ValueError, match="At least one merge key"):
        validate_merge_keys(bronze_rows_df, ())


def test_validate_merge_keys_reports_missing_keys(bronze_rows_df: DataFrame) -> None:
    with pytest.raises(ValueError, match="missing.*missing_key"):
        validate_merge_keys(bronze_rows_df, ("source_record_id", "missing_key"))


def test_create_delta_table_writes_rows_and_schema(
    spark: SparkSession,
    bronze_rows_df: DataFrame,
    tmp_path: Path,
) -> None:
    target = tmp_path / "bronze"

    create_delta_table(bronze_rows_df, target)

    actual = spark.read.format("delta").load(str(target))
    assert actual.schema.names == bronze_rows_df.schema.names
    assert [
        field.dataType for field in actual.schema.fields
    ] == [
        field.dataType for field in bronze_rows_df.schema.fields
    ]
    assert {
        tuple(row) for row in actual.collect()
    } == {
        ("erp_a", "id_1", "first"),
        ("erp_a", "id_2", "second"),
    }


def test_create_delta_table_supports_empty_dataframe(
    spark: SparkSession,
    tmp_path: Path,
) -> None:
    target = tmp_path / "empty-bronze"
    empty_df = spark.createDataFrame([], schema=SOURCE_SCHEMA)

    create_delta_table(empty_df, target)

    actual = spark.read.format("delta").load(str(target))
    assert actual.schema.names == SOURCE_SCHEMA.names
    assert [field.dataType for field in actual.schema.fields] == [
        field.dataType for field in SOURCE_SCHEMA.fields
    ]
    assert actual.count() == 0


def test_create_delta_table_does_not_overwrite_existing_target(
    spark: SparkSession,
    bronze_rows_df: DataFrame,
    tmp_path: Path,
) -> None:
    target = tmp_path / "bronze"
    create_delta_table(bronze_rows_df, target)

    replacement_df = _rows(spark, [("erp_a", "id_3", "replacement")])
    with pytest.raises(Exception):
        create_delta_table(replacement_df, target)

    actual = spark.read.format("delta").load(str(target))
    assert actual.count() == 2
    assert "id_3" not in {row.source_record_id for row in actual.collect()}


def test_merge_new_records_inserts_only_unmatched_composite_keys(
    spark: SparkSession,
    tmp_path: Path,
) -> None:
    target = tmp_path / "bronze"
    existing = _rows(spark, [("erp_a", "id_1", "original")])
    create_delta_table(existing, target)

    incoming = _rows(
        spark,
        [
            ("erp_a", "id_1", "changed-but-matched"),
            ("erp_a", "id_2", "new"),
            ("erp_b", "id_1", "new-source-system"),
        ],
    )
    merge_new_records(spark, incoming, target, COMPOSITE_KEYS)

    actual = spark.read.format("delta").load(str(target))
    rows = {
        (row._source_system, row.source_record_id): row.payload
        for row in actual.collect()
    }
    assert rows == {
        ("erp_a", "id_1"): "original",
        ("erp_a", "id_2"): "new",
        ("erp_b", "id_1"): "new-source-system",
    }


def test_merge_new_records_is_idempotent_on_replay(
    spark: SparkSession,
    tmp_path: Path,
) -> None:
    target = tmp_path / "bronze"
    incoming = _rows(spark, [("erp_a", "id_1", "value")])
    create_delta_table(incoming, target)

    merge_new_records(spark, incoming, target, COMPOSITE_KEYS)
    merge_new_records(spark, incoming, target, COMPOSITE_KEYS)

    assert spark.read.format("delta").load(str(target)).count() == 1


def test_merge_new_records_rejects_missing_merge_key(
    spark: SparkSession,
    tmp_path: Path,
) -> None:
    target = tmp_path / "bronze"
    existing = _rows(spark, [("erp_a", "id_1", "existing")])
    create_delta_table(existing, target)
    incoming = spark.createDataFrame(
        [("erp_a", "id_2", "incoming")],
        ["_source_system", "source_record_id", "payload"],
    ).drop("source_record_id")

    with pytest.raises(ValueError, match="source_record_id"):
        merge_new_records(spark, incoming, target, COMPOSITE_KEYS)


def test_write_bronze_data_creates_delta_target_on_first_write(
    spark: SparkSession,
    bronze_rows_df: DataFrame,
    tmp_path: Path,
) -> None:
    target = tmp_path / "bronze"

    write_bronze_data(spark, bronze_rows_df, target, COMPOSITE_KEYS)

    assert DeltaTable.isDeltaTable(spark, str(target))
    assert spark.read.format("delta").load(str(target)).count() == 2


def test_write_bronze_data_merges_new_keys_and_ignores_replay(
    spark: SparkSession,
    bronze_rows_df: DataFrame,
    tmp_path: Path,
) -> None:
    target = tmp_path / "bronze"
    write_bronze_data(spark, bronze_rows_df, target, COMPOSITE_KEYS)
    incoming = _rows(
        spark,
        [
            ("erp_a", "id_2", "should-not-update"),
            ("erp_b", "id_1", "new-source-system"),
        ],
    )

    first_write = write_bronze_data(spark, incoming, target, COMPOSITE_KEYS)
    replay_write = write_bronze_data(spark, incoming, target, COMPOSITE_KEYS)

    assert first_write.replay_existing_row_count == 1
    assert first_write.inserted_row_count == 1
    assert first_write.hash_conflict_count == 0
    assert first_write.target_total_row_count == 3
    assert replay_write.replay_existing_row_count == 2
    assert replay_write.inserted_row_count == 0
    assert replay_write.hash_conflict_count == 0
    assert replay_write.target_total_row_count == 3

    actual = spark.read.format("delta").load(str(target))
    rows = {
        (row._source_system, row.source_record_id): row.payload
        for row in actual.collect()
    }
    assert rows == {
        ("erp_a", "id_1"): "first",
        ("erp_a", "id_2"): "second",
        ("erp_b", "id_1"): "new-source-system",
    }


def test_write_bronze_data_rejects_existing_non_delta_path(
    spark: SparkSession,
    bronze_rows_df: DataFrame,
    tmp_path: Path,
) -> None:
    target = tmp_path / "not-delta"
    target.mkdir()
    marker = target / "keep.txt"
    marker.write_text("do not overwrite")

    with pytest.raises(ValueError, match="not a Delta table"):
        write_bronze_data(spark, bronze_rows_df, target, COMPOSITE_KEYS)

    assert marker.read_text() == "do not overwrite"