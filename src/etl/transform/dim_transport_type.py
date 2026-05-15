"""Build DIM_TRANSPORT_TYPE with hard-coded ADEME Base Carbone 2024 CO2 factors."""

from pyspark.sql import DataFrame, SparkSession

from models.schemas import DIM_TRANSPORT_TYPE_SCHEMA

# Columns: (SK, TRANSPORT_NAME, CO2_FACTOR_KG_PER_KM) — ADEME Base Carbone 2024
_ROWS: list[tuple[int, str, float]] = [
    (1, "Avion court-courrier", 0.230),
    (2, "Avion long-courrier", 0.187),
    (3, "Train", 0.006),
    (4, "Taxi", 0.192),
    (5, "Transports en commun", 0.029),
]


def build_dim_transport_type(spark: SparkSession) -> DataFrame:
    """Build DIM_TRANSPORT_TYPE from hard-coded CO2 factors (kg/km).

    Args:
        spark: Active SparkSession used to create the DataFrame.

    Returns:
        DataFrame matching DIM_TRANSPORT_TYPE_SCHEMA with 5 transport modes
        and their ADEME Base Carbone 2024 emission factors.
    """
    return spark.createDataFrame(_ROWS, schema=DIM_TRANSPORT_TYPE_SCHEMA)
