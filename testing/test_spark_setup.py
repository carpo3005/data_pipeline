import pytest

from pathlib import Path

from pyspark.sql import SparkSession
from src.spark_setup import get_spark_session


def test_spark_setup(spark):
    assert isinstance(spark, SparkSession), "The returned object is not a SparkSession."

def test_timzone_is_ust(spark):
    assert spark.conf.get("spark.sql.session.timeZone") == "UTC"

def test_delta_lake(spark, sample_df, tmp_path):
    
    table_path = str(tmp_path / "delta_table")
    sample_df.write.format("delta").save(table_path)
    result = spark.read.format("delta").load(table_path)

    assert result.count() == sample_df.count()

    