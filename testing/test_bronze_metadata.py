import pytest

import src.bronze_metadata as bronze_metadata

from pyspark.sql import DataFrame



def test_add_record_hash(spark):

    df = spark.createDataFrame(

    result = bronze_metadata.add_record_hash(

def test_add_ingestion_metadata():
    pass

def test_add_metadata_columns():
    pass