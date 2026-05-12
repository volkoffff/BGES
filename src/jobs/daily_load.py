"""Daily ETL: build augmented DIM_CITY, DIM_TRIP, FACT_MISSION and FACT_EQUIPMENT."""

import argparse
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, timedelta
import os
from pathlib import Path

os.environ.setdefault("PYARROW_IGNORE_TIMEZONE", "1")

from pyspark.sql import DataFrame, SparkSession

from config.settings import SITES, ETLConfig
from etl.extract.readers import read_semicolon_many
from etl.transform.dim_city import build_dim_city_augmented
from etl.transform.dim_trip import build_dim_trip
from etl.transform.fact_equipment import build_fact_equipment
from etl.transform.fact_mission import build_fact_mission
from jobs.initial_load import InitialTables, run_initial_load
from models.schemas import EQUIPMENT_RAW_SCHEMA, MISSION_RAW_SCHEMA
from utils.spark import get_spark


@dataclass
class DailyTables:
    """Tables produced by a single daily-load run."""

    dim_city: DataFrame
    dim_trip: DataFrame
    fact_mission: DataFrame
    fact_equipment: DataFrame


def _collect_paths(
    config: ETLConfig,
    date_start: date,
    date_end: date,
    file_fn: Callable[[str, str], Path],
) -> list[str]:
    """Return paths of all existing files for every site in [date_start, date_end].

    Files that do not exist for a given (site, date) pair are silently skipped so
    that gaps in the source data (e.g. weekends with no activity) do not cause
    errors at the Spark reader level.

    Args:
        config: ETL configuration used to resolve the base data directory.
        date_start: First date of the period to scan (inclusive).
        date_end: Last date of the period to scan (inclusive).
        file_fn: Callable mapping (site, date_str) to the expected file Path.

    Returns:
        List of string paths for files that exist on disk, in (date, site) order.
    """
    paths: list[str] = []
    current = date_start
    while current <= date_end:
        date_str = current.strftime("%Y%m%d")
        for site in SITES:
            p = file_fn(site, date_str)
            if p.exists():
                paths.append(str(p))
        current += timedelta(days=1)
    return paths


def run_daily_load(
    spark: SparkSession,
    config: ETLConfig,
    date_start: date,
    date_end: date,
    initial: InitialTables,
) -> DailyTables:
    """Build all daily tables (DIM_CITY, DIM_TRIP, FACT_MISSION, FACT_EQUIPMENT).

    Args:
        spark: Active SparkSession.
        config: ETL configuration providing source file paths and cache location.
        date_start: First date of the daily period to process (inclusive).
        date_end: Last date of the daily period to process (inclusive).
        initial: Static dimension tables produced by run_initial_load.

    Returns:
        DailyTables dataclass holding the four computed DataFrames.

    Raises:
        ValueError: When no mission files are found for the requested date range.
    """
    mission_paths = _collect_paths(config, date_start, date_end, config.mission_file)
    equipment_paths = _collect_paths(
        config, date_start, date_end, config.equipment_file
    )

    if not mission_paths:
        raise ValueError(
            f"No mission files found for {date_start} → {date_end}. "
            "Check --date-debut / --date-fin and the data directory."
        )

    sdf_missions_raw = read_semicolon_many(
        spark, mission_paths, schema=MISSION_RAW_SCHEMA
    )
    sdf_equipment_raw = read_semicolon_many(
        spark, equipment_paths, schema=EQUIPMENT_RAW_SCHEMA
    )

    sdf_dim_city = build_dim_city_augmented(
        spark, config, sdf_missions_raw, initial.dim_city
    ).cache()

    sdf_dim_trip = build_dim_trip(sdf_missions_raw, sdf_dim_city)

    sdf_fact_mission = build_fact_mission(
        sdf_missions_raw,
        initial.dim_date,
        initial.dim_staff,
        sdf_dim_trip,
        initial.dim_transport_type,
        sdf_dim_city,
    )

    sdf_fact_equipment = build_fact_equipment(
        sdf_equipment_raw,
        initial.dim_date,
        initial.dim_staff,
        initial.dim_equipment,
    )

    return DailyTables(
        dim_city=sdf_dim_city,
        dim_trip=sdf_dim_trip,
        fact_mission=sdf_fact_mission,
        fact_equipment=sdf_fact_equipment,
    )


def main() -> None:
    """CLI entry point: --date-debut YYYY-MM-DD --date-fin YYYY-MM-DD."""
    parser = argparse.ArgumentParser(description="BGES daily ETL")
    parser.add_argument("--date-debut", required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--date-fin", required=True, help="End date (YYYY-MM-DD)")
    args = parser.parse_args()

    date_start = date.fromisoformat(args.date_debut)
    date_end = date.fromisoformat(args.date_fin)

    spark = get_spark()
    config = ETLConfig()

    print("Running initial load…")
    initial = run_initial_load(spark, config)

    print(f"Running daily ETL {date_start} → {date_end}…")
    daily = run_daily_load(spark, config, date_start, date_end, initial)

    print(f"DIM_CITY      : {daily.dim_city.count()} cities")
    print(f"DIM_TRIP      : {daily.dim_trip.count()} trips")
    print(f"FACT_MISSION  : {daily.fact_mission.count()} missions")
    print(f"FACT_EQUIPMENT: {daily.fact_equipment.count()} purchases")

    spark.stop()


if __name__ == "__main__":
    main()
