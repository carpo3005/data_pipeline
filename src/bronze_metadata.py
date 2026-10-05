"""Raw-payload hashing and ingestion metadata enrichment."""

from pyspark.sql import functions as F
from pyspark.sql import DataFrame


def add_record_hash(df: DataFrame) -> DataFrame:
    """Add a deterministic hash of the raw source columns."""
    row_json = F.to_json(
        F.struct(*[F.col(name) for name in df.columns ]),
        options={"ignoreNullFields": "false"},
    )

    return (df
        .withColumn(
            "_record_hash",
            F.sha2(row_json, 256)
        ) 
    )

def add_ingestion_metadata(
    df: DataFrame,
    source_batch_id: str,
    run_id: str,
    source_system: str,
) -> DataFrame:
    """Add batch, run, source-system, and ingestion-time columns."""
    return (df
            .withColumn("_source_batch_id", F.lit(source_batch_id))
            .withColumn("_pipeline_run_id", F.lit(run_id))
            .withColumn("_source_system", F.lit(source_system))
            .withColumn("_ingested_timestamp", F.current_timestamp())
        )


def add_metadata_columns(
    df: DataFrame,
    source_batch_id: str,
    pipeline_run_id: str,
    source_system: str,
) -> DataFrame:
    """Add the record hash and ingestion metadata to the source DataFrame."""
    df = add_record_hash(df)
    df = add_ingestion_metadata(df, source_batch_id, pipeline_run_id, source_system)
    return df
