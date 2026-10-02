"""Data-contract and quality validation for bronze records."""

from dataclasses import dataclass

from pyspark.sql import DataFrame


@dataclass(frozen=True)
class ValidationMetrics:
    row_count: int
    null_or_blank_source_record_id_count: int
    duplicate_source_record_id_count: int
    corrupt_record_count: int
    null_record_hash_count: int


@dataclass(frozen=True)
class ValidationReport:
    entity: str
    metrics: ValidationMetrics
    errors: tuple[str, ...]

    @property
    def is_valid(self) -> bool:
        """Whether validation found any blocking errors."""
        raise NotImplementedError


def check_required_columns(df: DataFrame, entity: str) -> tuple[str, ...]:
    """Return required columns that are absent from the DataFrame."""
    raise NotImplementedError


def collect_validation_metrics(df: DataFrame) -> ValidationMetrics:
    """Collect row-level quality counts using Spark aggregations."""
    raise NotImplementedError


def apply_validation_policy(report: ValidationReport) -> None:
    """Raise or otherwise enforce the configured policy for a report."""
    raise NotImplementedError


def validate_bronze_data(df: DataFrame, entity: str) -> ValidationReport:
    """Build a validation report and enforce its blocking-error policy."""
    raise NotImplementedError
