from pyspark.sql import DataFrame, SparkSession

from models.schemas import DIM_TRANSPORT_TYPE_SCHEMA

# (SK, TRANSPORT_NAME, CO2_FACTOR_KG_PER_KM) — ADEME Base Carbone 2024
_ROWS = [
    (1, "Avion court-courrier", 0.230),
    (2, "Avion long-courrier", 0.187),
    (3, "Train", 0.006),
    (4, "Taxi", 0.192),
    (5, "Transports en commun", 0.029),
]


def build_dim_transport_type(spark: SparkSession) -> DataFrame:
    """Build DIM_TRANSPORT_TYPE from hard-coded CO2 factors (kg/km)."""
    return spark.createDataFrame(_ROWS, schema=DIM_TRANSPORT_TYPE_SCHEMA)
