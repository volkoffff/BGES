"""SparkSession factory for local development and script execution."""

import os

os.environ["PYARROW_IGNORE_TIMEZONE"] = "1"

from pyspark.sql import SparkSession


def get_spark(app_name: str = "BGES") -> SparkSession:
    """Create or retrieve the project's local SparkSession."""
    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.driver.host", "localhost")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
