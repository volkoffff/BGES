import argparse
from dataclasses import dataclass
from datetime import date, timedelta
import os

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
    """Tables produites par le chargement journalier."""

    dim_city: DataFrame
    dim_trip: DataFrame
    fact_mission: DataFrame
    fact_equipment: DataFrame


def _collect_paths(
    config: ETLConfig,
    date_start: date,
    date_end: date,
    file_fn: object,
) -> list[str]:
    """Collecter les chemins existants pour tous les sites dans la plage de dates."""
    paths = []
    current = date_start
    while current <= date_end:
        date_str = current.strftime("%Y%m%d")
        for site in SITES:
            p = file_fn(site, date_str)  # type: ignore[operator]
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
    """Orchestrer l'ETL journalier pour une plage de dates et retourner les tables."""
    mission_paths = _collect_paths(config, date_start, date_end, config.mission_file)
    equipment_paths = _collect_paths(
        config, date_start, date_end, config.equipment_file
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

    sdf_dim_trip = build_dim_trip(spark, sdf_missions_raw, sdf_dim_city)

    sdf_fact_mission = build_fact_mission(
        spark,
        sdf_missions_raw,
        initial.dim_date,
        initial.dim_staff,
        sdf_dim_trip,
        initial.dim_transport_type,
        sdf_dim_city,
    )

    sdf_fact_equipment = build_fact_equipment(
        spark,
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
    """Point d'entrée CLI : --date-debut YYYY-MM-DD --date-fin YYYY-MM-DD."""
    parser = argparse.ArgumentParser(description="ETL journalier BGES")
    parser.add_argument("--date-debut", required=True, help="Date début (YYYY-MM-DD)")
    parser.add_argument("--date-fin", required=True, help="Date fin (YYYY-MM-DD)")
    args = parser.parse_args()

    date_start = date.fromisoformat(args.date_debut)
    date_end = date.fromisoformat(args.date_fin)

    spark = get_spark()
    config = ETLConfig()

    print("Chargement initial…")
    initial = run_initial_load(spark, config)

    print(f"ETL journalier {date_start} → {date_end}…")
    daily = run_daily_load(spark, config, date_start, date_end, initial)

    print(f"DIM_CITY     : {daily.dim_city.count()} villes")
    print(f"DIM_TRIP     : {daily.dim_trip.count()} trajets")
    print(f"FACT_MISSION : {daily.fact_mission.count()} missions")
    print(f"FACT_EQUIPMENT: {daily.fact_equipment.count()} achats")

    spark.stop()


if __name__ == "__main__":
    main()
