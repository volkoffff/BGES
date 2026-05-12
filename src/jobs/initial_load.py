"""Initial load: build all static dimension tables from source files."""

from dataclasses import dataclass
from datetime import date

from pyspark.sql import DataFrame, SparkSession

from config.settings import ETLConfig
from etl.transform.dim_city import build_dim_city_initial
from etl.transform.dim_date import build_dim_date
from etl.transform.dim_equipment import build_dim_equipment
from etl.transform.dim_staff import build_dim_staff
from etl.transform.dim_transport_type import build_dim_transport_type


@dataclass
class InitialTables:
    """Container for the dimension tables produced by the initial load."""

    dim_date: DataFrame
    dim_transport_type: DataFrame
    dim_equipment: DataFrame
    dim_city: DataFrame
    dim_staff: DataFrame


def run_initial_load(
    spark: SparkSession,
    config: ETLConfig,
    date_start: date = date(2026, 1, 1),
    date_end: date = date(2027, 12, 31),
) -> InitialTables:
    """Build all static dimension tables and return them as a dataclass.

    Args:
        spark: Active SparkSession.
        config: ETL configuration providing all source file paths.
        date_start: First calendar day for DIM_DATE (default: 2026-01-01).
        date_end: Last calendar day for DIM_DATE (default: 2027-12-31).

    Returns:
        InitialTables dataclass holding the five dimension DataFrames:
        DIM_DATE, DIM_TRANSPORT_TYPE, DIM_EQUIPMENT, DIM_CITY and DIM_STAFF.
    """
    sdf_dim_date = build_dim_date(spark, date_start, date_end)
    sdf_dim_transport_type = build_dim_transport_type(spark)
    sdf_dim_equipment = build_dim_equipment(spark, config)
    sdf_dim_city = build_dim_city_initial(spark, config).cache()
    sdf_dim_staff = build_dim_staff(spark, config, sdf_dim_city)

    return InitialTables(
        dim_date=sdf_dim_date,
        dim_transport_type=sdf_dim_transport_type,
        dim_equipment=sdf_dim_equipment,
        dim_city=sdf_dim_city,
        dim_staff=sdf_dim_staff,
    )
