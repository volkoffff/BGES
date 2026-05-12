"""PySpark CSV reader helpers for semicolon- and comma-delimited source files."""

from collections.abc import Sequence
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType


def read_semicolon(
    spark: SparkSession,
    path: Path | str,
    schema: StructType | None = None,
) -> DataFrame:
    """Read a semicolon-delimited text file into a DataFrame.

    Args:
        spark: Active SparkSession.
        path: Path to the source file.
        schema: Optional StructType to enforce; inferred from header when None.

    Returns:
        DataFrame with one row per non-header line.
    """
    reader = (
        spark.read.option("header", "true")
        .option("sep", ";")
        .option("encoding", "UTF-8")
        .option("ignoreLeadingWhiteSpace", "true")
        .option("ignoreTrailingWhiteSpace", "true")
    )
    if schema:
        reader = reader.schema(schema)
    return reader.csv(str(path))


def read_comma(
    spark: SparkSession,
    path: Path | str,
    schema: StructType | None = None,
) -> DataFrame:
    """Read a comma-delimited CSV file into a DataFrame.

    Args:
        spark: Active SparkSession.
        path: Path to the source file.
        schema: Optional StructType to enforce; inferred from header when None.

    Returns:
        DataFrame with one row per non-header line.
    """
    reader = (
        spark.read.option("header", "true")
        .option("sep", ",")
        .option("encoding", "UTF-8")
        .option("ignoreLeadingWhiteSpace", "true")
        .option("ignoreTrailingWhiteSpace", "true")
    )
    if schema:
        reader = reader.schema(schema)
    return reader.csv(str(path))


def read_semicolon_many(
    spark: SparkSession,
    paths: Sequence[Path | str],
    schema: StructType | None = None,
) -> DataFrame:
    """Merge multiple semicolon-delimited files into a single DataFrame.

    Args:
        spark: Active SparkSession.
        paths: Sequence of file paths to read and union.
        schema: Optional StructType to enforce; inferred from header when None.

    Returns:
        DataFrame combining all rows from every supplied file.
    """
    reader = (
        spark.read.option("header", "true")
        .option("sep", ";")
        .option("encoding", "UTF-8")
        .option("ignoreLeadingWhiteSpace", "true")
        .option("ignoreTrailingWhiteSpace", "true")
    )
    if schema:
        reader = reader.schema(schema)
    return reader.csv([str(p) for p in paths])
