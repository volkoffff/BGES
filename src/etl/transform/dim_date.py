"""Build DIM_DATE by generating one row per calendar day for a given range."""

from datetime import date, timedelta

from pyspark.sql import DataFrame, SparkSession

from models.schemas import DIM_DATE_SCHEMA


def build_dim_date(
    spark: SparkSession,
    start: date = date(2026, 1, 1),
    end: date = date(2027, 12, 31),
) -> DataFrame:
    """Generate one DIM_DATE row per calendar day in [start, end].

    Args:
        spark: Active SparkSession used to create the DataFrame.
        start: First date to include, inclusive (default: 2026-01-01).
        end: Last date to include, inclusive (default: 2027-12-31).

    Returns:
        DataFrame matching DIM_DATE_SCHEMA with SK_DATE, DATE_ISO, YEAR,
        MONTH and DAY columns.
    """
    rows = []
    sk = 1
    current = start
    while current <= end:
        rows.append((sk, current, current.year, current.month, current.day))
        sk += 1
        current += timedelta(days=1)
    return spark.createDataFrame(rows, schema=DIM_DATE_SCHEMA)
