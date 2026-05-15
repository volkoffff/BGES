"""Build DIM_DATE by generating one row per calendar day for a given range."""

from datetime import date, timedelta

from pyspark.sql import DataFrame, SparkSession

from models.schemas import DIM_DATE_SCHEMA


def build_dim_date(
    spark: SparkSession,
    start: date = date(2026, 1, 1),
    end: date = date(2027, 12, 31),
) -> DataFrame:
    """Génère une ligne DIM_DATE par jour calendaire dans [start, end].

    Args:
        spark: SparkSession active.
        start: Premier jour inclus (défaut : 2026-01-01).
        end: Dernier jour inclus (défaut : 2027-12-31).

    Returns:
        DataFrame conforme à DIM_DATE_SCHEMA avec SK_DATE et DATE_ISO uniquement.
        YEAR, MONTH, DAY se calculent via F.year/F.month/F.dayofmonth(DATE_ISO).
    """
    rows: list[tuple[int, date]] = []
    sk: int = 1
    current: date = start
    while current <= end:
        rows.append((sk, current))
        sk += 1
        current += timedelta(days=1)
    return spark.createDataFrame(rows, schema=DIM_DATE_SCHEMA)
