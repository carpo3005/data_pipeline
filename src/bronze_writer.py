"""Delta table creation and idempotent-write operations."""

from dataclasses import dataclass
from functools import reduce
from operator import and_
from pathlib import Path

from pyspark.sql import Column, DataFrame, SparkSession, functions as F

from delta.tables import DeltaTable


@dataclass(frozen=True)
class WriteMetrics:
    replay_existing_row_count: int
    inserted_row_count: int
    hash_conflict_count: int
    target_total_row_count: int


def validate_merge_keys(df: DataFrame, merge_keys: tuple[str, ...]) -> None:
    """Raise when merge keys are empty or absent from the source DataFrame."""
    if not merge_keys:
        raise ValueError("At least one merge key is required")

    missing_keys = tuple(key for key in merge_keys if key not in df.columns)
    if missing_keys:
        raise ValueError(f"Merge key columns are missing from source: {missing_keys}")


def _build_merge_condition(merge_keys: tuple[str, ...]) -> Column:
    """Build a null-safe conjunction matching all configured merge keys."""
    return reduce(
        and_,
        [F.col(f"src.{key}").eqNullSafe(F.col(f"tgt.{key}")) for key in merge_keys],
    )


def _count_replays_and_hash_conflicts(
    source_df: DataFrame,
    target_df: DataFrame,
    matching_rows: DataFrame,
    matched_row_count: int,
) -> tuple[int, int]:
    """Count matching rows with equal hashes and conflicting hashes."""
    if "_record_hash" not in source_df.columns or "_record_hash" not in target_df.columns:
        return matched_row_count, 0

    same_hash = F.col("src._record_hash").eqNullSafe(F.col("tgt._record_hash"))
    replay_count = matching_rows.filter(same_hash).count()
    conflict_count = matching_rows.filter(~same_hash).count()
    return replay_count, conflict_count


def create_delta_table(
    df: DataFrame,
    target: Path,
) -> None:
    """Create the Delta table for its first write."""
    df.write.format("delta").mode("errorifexists").save(str(target))



def merge_new_records(
    spark: SparkSession,
    df: DataFrame,
    target: Path,
    merge_keys: tuple[str, ...],
) -> WriteMetrics:
    """Insert rows not already represented in the Delta target."""

    validate_merge_keys(df, merge_keys)
    delta_target = DeltaTable.forPath(spark, str(target)).alias("tgt")
    source_condition = _build_merge_condition(merge_keys)
    target_df = spark.read.format("delta").load(str(target)).alias("tgt")
    source_df = df.alias("src")
    matching_rows = source_df.join(target_df, source_condition, "inner")
    matched_row_count = matching_rows.count()
    replay_existing_row_count, hash_conflict_count = (
        _count_replays_and_hash_conflicts(
            source_df,
            target_df,
            matching_rows,
            matched_row_count,
        )
    )

    if hash_conflict_count:
        target_total_row_count = target_df.count()
        return WriteMetrics(
            replay_existing_row_count=replay_existing_row_count,
            inserted_row_count=0,
            hash_conflict_count=hash_conflict_count,
            target_total_row_count=target_total_row_count,
        )

    (
        delta_target.merge(source_df, source_condition)
        .whenNotMatchedInsertAll()
        .execute()
    )
    target_total_row_count = spark.read.format("delta").load(str(target)).count()
    return WriteMetrics(
        replay_existing_row_count=replay_existing_row_count,
        inserted_row_count=df.count() - matched_row_count,
        hash_conflict_count=0,
        target_total_row_count=target_total_row_count,
    )


def write_bronze_data(
    spark: SparkSession,
    df: DataFrame,
    target: Path,
    merge_keys: tuple[str, ...] = ("_source_system", "source_record_id"),
) -> WriteMetrics:
    """Create a Delta target or merge incoming rows into an existing one."""
    if DeltaTable.isDeltaTable(spark, str(target)):
        return merge_new_records(spark, df, target, merge_keys)
    elif target.exists():
        raise ValueError(f"Target path exists but is not a Delta table: {target}")
    else:
        validate_merge_keys(df, merge_keys)
        create_delta_table(df, target)
        inserted_row_count = df.count()
        return WriteMetrics(
            replay_existing_row_count=0,
            inserted_row_count=inserted_row_count,
            hash_conflict_count=0,
            target_total_row_count=inserted_row_count,
        )
