"""
Spark session factory.
Returns a configured SparkSession for local dev or Databricks runtime.
"""
from __future__ import annotations

import os
from pyspark.sql import SparkSession


def get_spark(app_name: str = "ForexETLPipeline") -> SparkSession:
    """
    Return a SparkSession.
    - On Databricks: reuses the running cluster session.
    - Locally: creates a local session with Delta support.
    """
    if _running_on_databricks():
        return SparkSession.builder.getOrCreate()

    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        # Required for Delta Lake on local machine
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0")
        .getOrCreate()
    )


def _running_on_databricks() -> bool:
    """Check if code is running inside a Databricks cluster."""
    return "DATABRICKS_RUNTIME_VERSION" in os.environ