import pytest
from pathlib import Path

from pyspark.sql import Row
from pyspark.sql.types import StringType, StructField, StructType
from pyspark.sql import DataFrame

from data_pipeline.src.bronze_source import load_entity_data, read_source_csv
from data_pipeline.src.spark_setup import get_spark_session

from data_pipeline.src.bronze_source import resolve_source_path
from data_pipeline.Configs.bronze_schemas import SCHEMAS
from data_pipeline.src.bronze_source import resolve_entity_schema

def test_resolve_source_path_uses_batch_and_entity(tmp_path):
    # Arrange: choose the inputs and expected result.
    batch_id = "B001"
    entity = "customer"
    expected = tmp_path / "B001" / "customer_B001.csv"

    # Act: call the function being tested.
    actual = resolve_source_path(tmp_path, batch_id, entity)

    # Assert: compare the result with what you expect.
    assert actual == expected

@pytest.mark.parametrize("entity, original_schema", SCHEMAS.items())
def test_resolve_entity_schema_adds_corrupt_record_field(entity, original_schema):
    actual = resolve_entity_schema(entity)

    expected_fields = list(original_schema.fields)
    if "_corrupt_record" not in original_schema.fieldNames():
        expected_fields.append(
            StructField("_corrupt_record", StringType(), True)
        )

    assert actual == StructType(expected_fields)

def test_resolve_entity_schema_does_not_duplicate_corrupt_record(monkeypatch):
    schema = StructType(
        list(SCHEMAS["customer"].fields)
        + [StructField("_corrupt_record", StringType(), True)]
    )
    monkeypatch.setitem(SCHEMAS, "test_entity", schema)

    actual = resolve_entity_schema("test_entity")

    assert actual == schema

@pytest.fixture(scope="module")
def spark():
    session = get_spark_session("test_bronze_source")
    yield session
    session.stop()

def test_read_source_csv_reads_headers_and_rows(tmp_path, spark):
    # Arrange: create a tiny CSV and the schema the reader should use.
    csv_path = tmp_path / "customer.csv"
    csv_path.write_text(
        "source_record_id,customer_id\nr1,c1\n",
        encoding="utf-8",
    )

    schema = StructType([
        StructField("source_record_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("_corrupt_record", StringType(), True),
    ])

    # Act: read the CSV.
    actual = read_source_csv(spark, schema, csv_path)

    # Assert: check the observable result.
    assert actual.columns == [
        "source_record_id",
        "customer_id",
        "_corrupt_record",
    ]
    assert actual.collect() == [
        Row(source_record_id="r1", customer_id="c1", _corrupt_record=None)
    ]


def test_read_source_csv_reads_multiline_quoted_field(tmp_path, spark):
    csv_path = tmp_path / "customer_multiline.csv"
    csv_path.write_text(
        'source_record_id,customer_id\nr1,"first line\nsecond line"\n',
        encoding="utf-8",
    )
    schema = StructType([
        StructField("source_record_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("_corrupt_record", StringType(), True),
    ])

    actual = read_source_csv(spark, schema, csv_path)

    assert actual.collect() == [
        Row(
            source_record_id="r1",
            customer_id="first line\nsecond line",
            _corrupt_record=None,
        )
    ]


def test_read_source_csv_captures_corrupt_record(tmp_path, spark):
    csv_path = tmp_path / "customer_corrupt.csv"
    corrupt_row = "r1,c1,unexpected"
    csv_path.write_text(
        f"source_record_id,customer_id\n{corrupt_row}",
        encoding="utf-8",
    )
    schema = StructType([
        StructField("source_record_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("_corrupt_record", StringType(), True),
    ])

    actual = read_source_csv(spark, schema, csv_path)

    rows = actual.collect()
    assert len(rows) == 1
    assert rows[0]["_corrupt_record"] == corrupt_row


def test_load_entity_data(spark):
    
    df = load_entity_data(
        spark,
        data_root=Path("data_pipeline/data"),
        batch_id="B001",
        entity="customer",
    )

    assert isinstance(df, DataFrame)
    #assert df.count() == 2