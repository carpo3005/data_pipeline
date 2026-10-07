import pytest

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType

from Configs.bronze_schemas import SCHEMAS
from src.bronze_validation import (
    ValidationMetrics,
    ValidationReport,
    check_required_columns,
    collect_validation_metrics,
    apply_validation_policy,
    validate_bronze_data,
    _build_error_report,
)

METADATA_COLUMNS = (
    "_record_hash",
    "_source_batch_id",
    "_pipeline_run_id",
    "_source_system",
    "_ingested_timestamp",
)


@pytest.fixture
def metadata_columns() -> list[str]:
    return list(METADATA_COLUMNS)


@pytest.fixture(params=tuple(SCHEMAS))
def entity(request: pytest.FixtureRequest) -> str:
    return request.param


@pytest.fixture
def source_contract(entity: str) -> StructType:
    return SCHEMAS[entity]


@pytest.fixture
def required_columns(
    metadata_columns: list[str],
    source_contract: StructType,
) -> list[str]:
    return (
        source_contract.fieldNames()
        + ["_corrupt_record"]
        + metadata_columns
    )


@pytest.fixture
def required_schema(required_columns: list[str]) -> StructType:
    return StructType(
        [StructField(column, StringType(), True) for column in required_columns]
    )


@pytest.fixture
def complete_bronze_df(spark: SparkSession, required_schema: StructType) -> DataFrame:
    return spark.createDataFrame([], required_schema)


def test_check_required_columns_complete_dataframe(
    entity: str, complete_bronze_df: DataFrame
) -> None:
    assert check_required_columns(complete_bronze_df, entity) == ()


def test_check_required_columns_missing_source_column(
    entity: str, complete_bronze_df: DataFrame
) -> None:
    df_missing_source_column = complete_bronze_df.drop("source_record_id")
    assert check_required_columns(df_missing_source_column, entity) == ("source_record_id",)


@pytest.mark.parametrize("metadata_column", METADATA_COLUMNS)
def test_check_required_columns_missing_metadata_column(
    entity: str,
    metadata_column: str,
    complete_bronze_df: DataFrame,
) -> None:
    df_missing_metadata_column = complete_bronze_df.drop(metadata_column)
    assert check_required_columns(df_missing_metadata_column, entity) == (
        metadata_column,
    )



def test_check_required_columns_allows_extra_column(
    entity: str,
    complete_bronze_df: DataFrame,
) -> None:
    df_with_extra_column = complete_bronze_df.withColumn(
        "unexpected_column",
        F.lit("extra"),
    )

    assert check_required_columns(df_with_extra_column, entity) == ()

def test_check_required_columns_unknown_entity(complete_bronze_df: DataFrame) -> None:
    with pytest.raises(ValueError, match="Unknown entity"):
        check_required_columns(complete_bronze_df, "not_an_entity")

# ===================================================================
# Testing collect_validation_metrics
# ===================================================================

@pytest.fixture
def dirty_df(spark: SparkSession) -> DataFrame:
    return spark.createDataFrame(
            [
                ("id_1", None, "hash_1"),
                ("id_1", None, None),
                ("", "bad_row", "hash_3"),
                (None, None, None),
                ("id_4", None, "hash_4"),
            ],
            ("source_record_id", "_corrupt_record", "_record_hash")
        )

@pytest.fixture
def dirty_validation_metrics() -> ValidationMetrics:
    return ValidationMetrics(
        row_count=5,
        null_or_blank_source_record_id_count=2,
        distinct_duplicated_source_record_id_count=1,
        corrupt_record_count=1,
        null_record_hash_count=2,
        duplicate_rows_beyond_first_count=1,
    )

@pytest.fixture
def dirty_values(dirty_df, dirty_validation_metrics) -> tuple[DataFrame, ValidationMetrics]:
    return dirty_df, dirty_validation_metrics

@pytest.fixture
def clean_df(spark: SparkSession) -> DataFrame:
    schema = StructType(
        [
            StructField("source_record_id", StringType(), True),
            StructField("_corrupt_record", StringType(), True),
            StructField("_record_hash", StringType(), True),
        ]
    )
    return spark.createDataFrame(
            [
                ("id_1", None, "hash_1"),
                ("id_2", None, "hash_2"),
                ("id_3", None, "hash_3"),
                ("id_4", None, "hash_4"),
                ("id_5", None, "hash_5"),
            ],
            schema=schema,
        )

@pytest.fixture
def clean_validation_metrics() -> ValidationMetrics:
    return ValidationMetrics(
        row_count=5,
        null_or_blank_source_record_id_count=0,
        distinct_duplicated_source_record_id_count=0,
        corrupt_record_count=0,
        null_record_hash_count=0,
    )

@pytest.fixture
def clean_values(clean_df, clean_validation_metrics) -> tuple[DataFrame, ValidationMetrics]:
    return clean_df, clean_validation_metrics

@pytest.mark.parametrize(
    "case_fixture",
    ["dirty_values", "clean_values"],
    ids=["dirty", "clean"],
)
def test_collect_validation_metrics(
    request: pytest.FixtureRequest,
    case_fixture: str,
) -> None:
    df, expected_metrics = request.getfixturevalue(case_fixture)

    actual_metrics = collect_validation_metrics(df)

    assert actual_metrics == expected_metrics, (
        "Validation metrics were not calculated correctly"
    )


def test_collect_validation_metrics_counts_distinct_duplicated_source_record_ids(
    spark: SparkSession,
) -> None:
    schema = StructType(
        [
            StructField("source_record_id", StringType(), True),
            StructField("_corrupt_record", StringType(), True),
            StructField("_record_hash", StringType(), True),
        ]
    )
    df = spark.createDataFrame(
        [
            ("id_1", None, "hash_1"),
            ("id_1", None, "hash_2"),
            ("id_2", None, "hash_3"),
            ("id_2", None, "hash_4"),
            ("id_2", None, "hash_5"),
            ("id_3", None, "hash_6"),
        ],
        schema=schema,
    )

    actual_metrics = collect_validation_metrics(df)

    assert actual_metrics.distinct_duplicated_source_record_id_count == 2


def test_collect_validation_metrics_handles_empty_data_frame(spark: SparkSession) -> None:
   
    empty_schema = StructType([
    StructField("source_record_id", StringType(), True),
    StructField("_corrupt_record", StringType(), True),
    StructField("_record_hash", StringType(), True),
    ])

    empty_df = spark.createDataFrame([], schema=empty_schema)

    expected = ValidationMetrics(
        row_count=0,
        null_or_blank_source_record_id_count=0,
        distinct_duplicated_source_record_id_count=0,
        corrupt_record_count=0,
        null_record_hash_count=0,
    )

    actual = collect_validation_metrics(empty_df)

    assert actual == expected, (
        f"edge case where dataframe is empty failed "
        f"\n expected: {expected} \n actual: {actual}"
    )

# ===================================================================
# Testing apply_validation_policy
# ===================================================================

@pytest.fixture
def clean_validation_report() -> ValidationReport:
    clean_metrics = ValidationMetrics(
        row_count=5,
        null_or_blank_source_record_id_count=0,
        distinct_duplicated_source_record_id_count=0,
        corrupt_record_count=0,
        null_record_hash_count=0,
    )
    return ValidationReport(
        entity="customer",
        metrics=clean_metrics,
        errors=(),
    )


def test_apply_validation_policy_does_not_raise_for_valid_report(
    clean_validation_report: ValidationReport,
) -> None:
    apply_validation_policy(clean_validation_report)


def test_apply_validation_policy_raises_for_report_with_error() -> None:
    metrics = ValidationMetrics(
        row_count=5,
        null_or_blank_source_record_id_count=0,
        distinct_duplicated_source_record_id_count=0,
        corrupt_record_count=0,
        null_record_hash_count=0,
    )
    report = ValidationReport(
        entity="customer",
        metrics=metrics,
        errors=("Missing required column: source_record_id",),
    )

    with pytest.raises(ValueError):
        apply_validation_policy(report)


def test_apply_validation_policy_error_identifies_entity_and_error() -> None:
    metrics = ValidationMetrics(
        row_count=5,
        null_or_blank_source_record_id_count=0,
        distinct_duplicated_source_record_id_count=0,
        corrupt_record_count=0,
        null_record_hash_count=0,
    )
    error = "Missing required column: source_record_id"
    report = ValidationReport(
        entity="customer",
        metrics=metrics,
        errors=(error,),
    )

    with pytest.raises(ValueError) as exc_info:
        apply_validation_policy(report)

    assert "customer" in str(exc_info.value)
    assert error in str(exc_info.value)


def test_apply_validation_policy_error_includes_all_reported_errors() -> None:
    metrics = ValidationMetrics(
        row_count=5,
        null_or_blank_source_record_id_count=0,
        distinct_duplicated_source_record_id_count=0,
        corrupt_record_count=0,
        null_record_hash_count=0,
    )
    errors = (
        "Missing required column: source_record_id",
        "Found null _record_hash values",
    )
    report = ValidationReport(
        entity="customer",
        metrics=metrics,
        errors=errors,
    )

    with pytest.raises(ValueError) as exc_info:
        apply_validation_policy(report)

    exception_message = str(exc_info.value)
    for error in errors:
        assert error in exception_message


def test_build_error_report_returns_empty_tuple_for_zero_quality_metrics() -> None:
    metrics = ValidationMetrics(
        row_count=5,
        null_or_blank_source_record_id_count=0,
        distinct_duplicated_source_record_id_count=0,
        corrupt_record_count=0,
        null_record_hash_count=0,
    )

    assert _build_error_report(metrics, missing_columns=()) == ()


def test_build_error_report_includes_each_nonzero_quality_metric() -> None:
    metrics = ValidationMetrics(
        row_count=5,
        null_or_blank_source_record_id_count=2,
        distinct_duplicated_source_record_id_count=1,
        corrupt_record_count=3,
        null_record_hash_count=4,
    )

    errors = _build_error_report(metrics, missing_columns=())

    assert len(errors) == 3
    error_text = " ".join(errors)
    assert "2" in error_text
    assert "1" in error_text
    assert "4" in error_text
    assert "null or blank source_record_id" in error_text
    assert "distinct duplicated source_record_id" in error_text
    assert "null _record_hash" in error_text


def test_build_error_report_includes_missing_required_columns() -> None:
    metrics = ValidationMetrics(
        row_count=5,
        null_or_blank_source_record_id_count=0,
        distinct_duplicated_source_record_id_count=0,
        corrupt_record_count=0,
        null_record_hash_count=0,
    )

    errors = _build_error_report(
        metrics,
        missing_columns=("source_record_id", "_source_system"),
    )

    assert errors == (
        "Missing required column: source_record_id",
        "Missing required column: _source_system",
    )


def test_apply_validation_policy_warns_for_corrupt_records_without_raising() -> None:
    metrics = ValidationMetrics(
        row_count=5,
        null_or_blank_source_record_id_count=0,
        distinct_duplicated_source_record_id_count=0,
        corrupt_record_count=3,
        null_record_hash_count=0,
    )
    report = ValidationReport(
        entity="customer",
        metrics=metrics,
        errors=(),
    )

    assert report.is_valid
    with pytest.warns(UserWarning, match="3 corrupt records"):
        apply_validation_policy(report)


# ===================================================================
# Testing validate_bronze_data
# ===================================================================

def _customer_bronze_df(
    spark: SparkSession,
    rows: list[tuple[str | None, str | None, str | None]],
    *,
    missing_columns: tuple[str, ...] = (),
) -> DataFrame:
    metrics_schema = StructType(
        [
            StructField("source_record_id", StringType(), True),
            StructField("_corrupt_record", StringType(), True),
            StructField("_record_hash", StringType(), True),
        ]
    )
    df = spark.createDataFrame(rows, schema=metrics_schema)
    required_columns = (
        SCHEMAS["customer"].fieldNames()
        + ["_corrupt_record"]
        + list(METADATA_COLUMNS)
    )

    for column in required_columns:
        if column not in df.columns and column not in missing_columns:
            df = df.withColumn(column, F.lit(None).cast(StringType()))

    present_required_columns = [
        column for column in required_columns if column not in missing_columns
    ]
    return df.select(*present_required_columns)


def test_validate_bronze_data_returns_report_for_clean_dataframe(
    spark: SparkSession,
) -> None:
    df = _customer_bronze_df(
        spark,
        [("id_1", None, "hash_1")],
    )

    report = validate_bronze_data(df, "customer")

    assert report.entity == "customer"
    assert report.metrics == ValidationMetrics(
        row_count=1,
        null_or_blank_source_record_id_count=0,
        distinct_duplicated_source_record_id_count=0,
        corrupt_record_count=0,
        null_record_hash_count=0,
    )
    assert report.errors == ()


def test_validate_bronze_data_returns_report_for_quality_issues(
    spark: SparkSession,
) -> None:
    df = _customer_bronze_df(
        spark,
        [
            ("id_1", None, "hash_1"),
            ("id_1", None, "hash_2"),
            ("", "bad_row", None),
            (None, None, None),
        ],
    )

    report = validate_bronze_data(df, "customer")

    assert report.metrics == ValidationMetrics(
        row_count=4,
        null_or_blank_source_record_id_count=2,
        distinct_duplicated_source_record_id_count=1,
        corrupt_record_count=1,
        null_record_hash_count=2,
        duplicate_rows_beyond_first_count=1,
    )
    assert len(report.errors) == 3
    error_text = " ".join(report.errors).lower()
    assert "source_record_id" in error_text
    assert "duplicat" in error_text
    assert "corrupt" not in error_text
    assert "_record_hash" in error_text


def test_validate_bronze_data_reports_missing_required_column(
    spark: SparkSession,
) -> None:
    df = _customer_bronze_df(
        spark,
        [("id_1", None, "hash_1")],
        missing_columns=("_source_system",),
    )

    report = validate_bronze_data(df, "customer")

    assert report.metrics.row_count == 1
    assert len(report.errors) == 1
    assert "_source_system" in report.errors[0]


def test_validate_bronze_data_raises_for_unknown_entity(
    spark: SparkSession,
) -> None:
    df = _customer_bronze_df(
        spark,
        [("id_1", None, "hash_1")],
    )

    with pytest.raises(ValueError, match="Unknown entity"):
        validate_bronze_data(df, "not_an_entity")
