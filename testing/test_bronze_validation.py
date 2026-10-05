import pytest
from pyspark.sql import DataFrame
from pyspark.sql.types import StringType, StructField, StructType

from Configs.bronze_schemas import SCHEMAS
from src.bronze_validation import (
    ValidationMetrics,
    ValidationReport,
    check_required_columns,
    collect_validation_metrics,
    apply_validation_policy,
    validate_bronze_data,
)

@pytest.fixture
def metadata_columns() -> list[str]:
    return [
        "_record_hash",
        "_source_batch_id",
        "_pipeline_run_id",
        "_source_system",
        "_ingested_timestamp",
    ]

@pytest.fixture
def source_contract(request: pytest.FixtureRequest) -> StructType:
    return SCHEMAS[request.param]


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
def complete_bronze_df(spark, required_schema: StructType) -> DataFrame:
    return spark.createDataFrame([], required_schema)


# def test_contract_has_source_record_id(source_contract):
#     assert "source_record_id" in source_contract.fieldNames()

@pytest.mark.parametrize(
    "entity, source_contract",
    [("customer", "customer"), ("order", "order")],
    indirect=["source_contract"],
)
def test_check_required_columns_complete_dataframe(
    entity: str, complete_bronze_df: DataFrame
) -> None:
    assert check_required_columns(complete_bronze_df, entity) == ()


@pytest.mark.parametrize(
    "entity, source_contract",
    [("customer", "customer"), ("order", "order")],
    indirect=["source_contract"],
)
def test_check_required_columns_missing_source_column(
    entity: str, complete_bronze_df: DataFrame
) -> None:
    df_missing_source_column = complete_bronze_df.drop("source_record_id")
    assert check_required_columns(df_missing_source_column, entity) == (
        "source_record_id",
    )

def test_check_required_columns_missing_metadata_column():
    pass

def test_check_required_columns_extra_column():
    pass

def test_check_required_columns_unknown_entity():
    pass