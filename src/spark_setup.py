
from pathlib import Path
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

def get_spark_session(app_name: str = "data_pipeline") -> SparkSession:
    """
    Create and return a SparkSession with the specified application name.

    Args:
        app_name (str): The name of the Spark application.
    """
    warehouse_path = Path.home() / ".local/share/Project_Mercury/spark-warehouse"

    builder = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", warehouse_path.as_uri())
    )

    return configure_spark_with_delta_pip(builder).getOrCreate()