import re
import pytest
from datetime import datetime


from src.bronze_metadata import (
    add_record_hash,
    add_ingestion_metadata,
    add_metadata_columns,
)



def test_add_record_hash(sample_df):

    df_with_hash = add_record_hash(sample_df)

    #Check hash column exists
    assert "_record_hash" in df_with_hash.columns

def test_add_record_hash_is_deterministic(duplicate_rows_df):

    # Check two records result in the same hash
    hashed_record = add_record_hash(duplicate_rows_df)
    hashed_records = [row["_record_hash"] for row in hashed_record.select("_record_hash").collect()]

    assert hashed_records[0] == hashed_records[1] , (
        "Identical input rows should produce the same _record_hash"
    )

def test_add_record_hash_is_sha256(sample_df):
    df_with_hash = add_record_hash(sample_df)

    for row in df_with_hash.select("_record_hash").collect():
        record_hash = row["_record_hash"]
        assert isinstance(record_hash, str), "Hash should be string format"
        assert re.fullmatch(r"[0-9a-f]{64}", record_hash), (f"Expected a 64-character SHA-256 hex hash; got {hash!r}")

def test_add_record_hash_differs(diff_df):
    df_with_hash = add_record_hash(diff_df)
    hashed_records = [row["_record_hash"] for row in df_with_hash.select("_record_hash").collect()]
    assert hashed_records[0] != hashed_records[1], "different values have same hash"
    assert len(hashed_records) == 2, f"Issue with the test there should only be two records there are {len(records)}"

@pytest.fixture(scope="session")
def df_with_metadata(sample_df, ingestion_config, pipeline_run_id):
    return add_ingestion_metadata(
        sample_df,
        source_batch_id=ingestion_config.source_batch_id,
        run_id=pipeline_run_id,
        source_system=ingestion_config.source_system,
    )

def test_add_ingestion_metadata_adds_correct_columns(df_with_metadata):

    metadata_columns = df_with_metadata.columns
    required_columns = ["_source_batch_id", "_pipeline_run_id", "_source_system", "_ingested_timestamp"]

    for req_col in required_columns:
        assert req_col in metadata_columns, (
            f"{req_col} not in metadata columns"
        )

def test_add_ingestion_metadata_adds_correct_data(df_with_metadata, ingestion_config, pipeline_run_id):
    
    row = df_with_metadata.select(
    "_source_batch_id", "_pipeline_run_id", "_source_system"
    ).first()

    assert row["_source_batch_id"] == ingestion_config.source_batch_id
    assert row["_source_system"]   == ingestion_config.source_system
    assert row["_pipeline_run_id"] == pipeline_run_id

def test_add_ingestion_metadata_timestamp_validity(df_with_metadata):
    ingestion_timestamp = (
        df_with_metadata.select("_ingested_timestamp").first()["_ingested_timestamp"]
    )

    assert isinstance(ingestion_timestamp, datetime), "ingestion timestamp is not a valid timestamp value"


def test_add_ingestion_metadata_timestamp_is_correct(
    spark, sample_df, ingestion_config, pipeline_run_id
):
    before = spark.sql("SELECT current_timestamp() AS now").first()["now"]
    df_with_metadata = add_ingestion_metadata(
        sample_df,
        source_batch_id=ingestion_config.source_batch_id,
        run_id=pipeline_run_id,
        source_system=ingestion_config.source_system,
    )
    timestamp = (
        df_with_metadata.select("_ingested_timestamp").first()["_ingested_timestamp"]
    )
    after = spark.sql("SELECT current_timestamp() AS now").first()["now"]

    assert before <= timestamp <= after, "timestamp time is not in sync with spark's time"

@pytest.fixture(scope='session')
def metadata_enriched_df(sample_df, ingestion_config, pipeline_run_id):
    """Return sample data with hash and ingestion metadata added."""
    return add_metadata_columns(
        sample_df,
        ingestion_config.source_batch_id,
        pipeline_run_id,
        ingestion_config.source_system,
    )


def test_add_metadata_columns_preserves_source_columns_and_adds_metadata(
    sample_df, metadata_enriched_df
):
    required_columns = [
        "_record_hash",
        "_source_batch_id",
        "_pipeline_run_id",
        "_ingested_timestamp",
    ]
    result_columns = set(metadata_enriched_df.columns)

    assert set(required_columns) <= result_columns, (
        f"Missing metadata columns: {set(required_columns) - result_columns}"
    )
    assert set(sample_df.columns) <= result_columns, (
        f"Missing source columns: {set(sample_df.columns) - result_columns}"
    )

def test_add_metadata_columns_hashes_first(sample_df, ingestion_config, pipeline_run_id):
    
    expected = sorted(
        row["_record_hash"] 
        for row in add_record_hash(sample_df).select("_record_hash").collect()
    )

    enriched = add_metadata_columns(
        sample_df,
        source_batch_id=ingestion_config.source_batch_id,
        pipeline_run_id=pipeline_run_id,
        source_system=ingestion_config.source_system,
    )
    
    actual = sorted(
        row["_record_hash"]
        for row in enriched.select("_record_hash").collect()
    )

    assert actual == expected, (
        "Hashes should be computed from source columns only"
    )


def test_add_metadata_columns_returns_accurately(
    metadata_enriched_df, ingestion_config, pipeline_run_id
):
    row = metadata_enriched_df.select(
        "_source_batch_id", "_pipeline_run_id", "_source_system"
    ).first()

    assert row["_source_batch_id"] == ingestion_config.source_batch_id
    assert row["_pipeline_run_id"] == pipeline_run_id
    assert row["_source_system"] == ingestion_config.source_system