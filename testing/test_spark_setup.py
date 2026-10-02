import pytest
from pyspark.sql import SparkSession
from data_pipeline.src.spark_setup import get_spark_session

def test_spark_setup():
    """
    Test to ensure that the Spark session is set up correctly.
    """
    spark = get_spark_session("test_app")
    assert isinstance(spark, SparkSession), "The returned object is not a SparkSession."