"""SparkSession factory for local development and script execution."""

import os

os.environ["PYARROW_IGNORE_TIMEZONE"] = "1"
# Tells Spark to bind directly to loopback instead of resolving the hostname —
# suppresses "hostname resolves to a loopback address" on WSL/Docker setups.
os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")

from pyspark.sql import SparkSession


def get_spark(app_name: str = "BGES") -> SparkSession:
    """Create or retrieve the project's local SparkSession.

    Args:
        app_name: Spark application name shown in the UI (default: "BGES").

    Returns:
        Active SparkSession configured for local mode with the UI disabled.
    """
    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.driver.host", "localhost")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
