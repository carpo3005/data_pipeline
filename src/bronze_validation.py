"""Data-contract and quality validation for bronze records."""

from dataclasses import dataclass
import warnings

from pyspark.sql import functions as F
from pyspark.sql import DataFrame

from Configs.bronze_schemas import SCHEMAS
from Configs.metadata_columns import METADATA_COLUMNS



@dataclass(frozen=True)
class ValidationMetrics:
    row_count: int
    null_or_blank_source_record_id_count: int | None
    distinct_duplicated_source_record_id_count: int | None
    corrupt_record_count: int
    null_record_hash_count: int
    duplicate_rows_beyond_first_count: int | None = 0


@dataclass(frozen=True)
class ValidationReport:
    entity: str
    metrics: ValidationMetrics
    errors: tuple[str, ...]

    @property
    def is_valid(self) -> bool:
        """Whether validation found any blocking errors."""
        return not self.errors


def check_required_columns(df: DataFrame, entity: str) -> tuple[str, ...]:
    """Return required columns that are absent from the DataFrame."""
    if entity not in SCHEMAS:
        raise ValueError(f"Unknown entity: {entity}")

    required_columns = (
        list(METADATA_COLUMNS)
        + ["_corrupt_record"]
        + SCHEMAS[entity].fieldNames()
    )

    dataframe_columns = df.columns
    return tuple(
        column for column in required_columns if column not in dataframe_columns
    )


def collect_validation_metrics(df: DataFrame) -> ValidationMetrics:
    """Collect row-level quality counts using Spark aggregations."""
    cached_df = df.cache()
    try:
        row_count = cached_df.count()
        if "source_record_id" in cached_df.columns:
            duplicate_id_groups = _count_distinct_duplicated_source_ids(cached_df)
            duplicate_rows_beyond_first = _count_duplicate_rows_beyond_first(cached_df)
            null_id_count = _count_null_ids(cached_df)
        else:
            duplicate_id_groups = None
            duplicate_rows_beyond_first = None
            null_id_count = None
        corrupt_record_count = _count_corrupt_records(cached_df)
        null_record_hash_count = _count_null_hashes(cached_df)

        return ValidationMetrics(
            row_count=row_count,
            null_or_blank_source_record_id_count=null_id_count,
            distinct_duplicated_source_record_id_count=duplicate_id_groups,
            corrupt_record_count=corrupt_record_count,
            null_record_hash_count=null_record_hash_count,
            duplicate_rows_beyond_first_count=duplicate_rows_beyond_first,
        )
    finally:
        cached_df.unpersist()


def apply_validation_policy(report: ValidationReport) -> None:
    """Raise or otherwise enforce the configured policy for a report."""
    if report.metrics.corrupt_record_count:
        warnings.warn(
            f"Bronze validation found {report.metrics.corrupt_record_count} "
            f"corrupt records for entity '{report.entity}'",
            UserWarning,
            stacklevel=2,
        )

    if report.errors:
        error_details = "\n".join(f"- {error}" for error in report.errors)
        raise ValueError(
            f"Bronze validation failed for entity '{report.entity}':\n"
            f"{error_details}"
        )


def validate_bronze_data(df: DataFrame, entity: str) -> ValidationReport:
    """Build a validation report and enforce its blocking-error policy."""
    if entity not in SCHEMAS:
            raise ValueError(f"Unknown entity: {entity}")

    missing_columns = check_required_columns(df, entity)
    validation_metrics = collect_validation_metrics(df)
    errors = _build_error_report(validation_metrics, missing_columns)

    validation_report = ValidationReport(
        entity=entity,
        metrics=validation_metrics,
        errors=errors,
    )

    return validation_report


def _count_distinct_duplicated_source_ids(df: DataFrame) -> int:
    nonblank_ids = df.filter(
        F.col("source_record_id").isNotNull()
        & (F.col("source_record_id") != "")
    )

    return (
        nonblank_ids
        .groupBy("source_record_id")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )


def _count_duplicate_rows_beyond_first(df: DataFrame) -> int:
    nonblank_ids = df.filter(
        F.col("source_record_id").isNotNull()
        & (F.col("source_record_id") != "")
    )
    duplicate_group_counts = (
        nonblank_ids
        .groupBy("source_record_id")
        .count()
        .filter(F.col("count") > 1)
    )
    return duplicate_group_counts.agg(
        F.coalesce(F.sum(F.col("count") - 1), F.lit(0)).alias("duplicate_rows")
    ).first()["duplicate_rows"]
def _count_null_ids(df: DataFrame) -> int:
    return (
        df
        .filter(
            F.col("source_record_id").isNull()
            | (F.col("source_record_id") == "")
        )
    ).count()

def _count_corrupt_records(df: DataFrame) -> int:
    return (
        df.filter(F.col("_corrupt_record").isNotNull())
        .count()
    )

def _count_null_hashes(df: DataFrame) -> int:
    return (
        df
        .filter(F.col("_record_hash").isNull())
        .count()
    )

def _build_error_report(validation_metrics: ValidationMetrics, missing_columns: tuple[str, ...]) -> tuple[str, ...]:
    errors: list[str] = []

    if validation_metrics.null_or_blank_source_record_id_count:
        errors.append(
            "Found "
            f"{validation_metrics.null_or_blank_source_record_id_count} "
            "rows with null or blank source_record_id values"
        )

    if validation_metrics.distinct_duplicated_source_record_id_count:
        errors.append(
            "Found "
            f"{validation_metrics.distinct_duplicated_source_record_id_count} "
            "distinct duplicated source_record_id values"
        )

    if validation_metrics.null_record_hash_count:
        errors.append(
            f"Found {validation_metrics.null_record_hash_count} rows with null _record_hash"
        )

    if missing_columns:
        errors.extend(
            f"Missing required column: {column}" for column in missing_columns
        )
    return tuple(errors)