"""Source path, schema, and CSV-loading operations for bronze ingestion."""

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StringType, StructField, StructType

from data_pipeline.Configs.bronze_schemas import SCHEMAS


def resolve_source_path(data_root: Path, batch_id: str, entity: str) -> Path:
    """Build the source CSV path for one entity and batch."""
    source_path = data_root / batch_id / f"{entity}_{batch_id}.csv"
    return source_path

def resolve_entity_schema(entity: str) -> StructType:
    """Return the expected raw schema, including the corrupt-record field."""
    
    schema = SCHEMAS[entity]
    if "_corrupt_record" not in schema.fieldNames():
        return StructType(schema.fields + [StructField("_corrupt_record", StringType(), True)])

    return schema


def read_source_csv(
    spark: SparkSession,
    schema: StructType,
    path: Path,
) -> DataFrame:
    """Read one source CSV with the ingestion CSV options."""
    
    df = (
        spark.read
        .option("header", "true")
        .schema(schema)
        .option("delimiter", ",")
        .option("multiline", "true")
        .option("mode", "permissive")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .csv(str(path))
    )
    return df


def load_entity_data(
    spark: SparkSession,
    data_root: Path,
    batch_id: str,
    entity: str,
) -> DataFrame:
    """Resolve the path and schema, then read one entity's source CSV."""
    source_path = resolve_source_path(data_root, batch_id, entity)
    schema = resolve_entity_schema(entity)
    return read_source_csv(spark, schema, source_path)

