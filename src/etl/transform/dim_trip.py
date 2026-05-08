"""Build DIM_TRIP: distinct (origin, destination) city pairs with geodesic distance."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
from pyspark.sql.window import Window

from models.schemas import DIM_TRIP_SCHEMA
from utils.distance import geodesic_km


def build_dim_trip(
    sdf_missions_raw: DataFrame,
    sdf_dim_city: DataFrame,
) -> DataFrame:
    """Return one row per distinct (origin, destination) pair with DISTANCE_KM.

    The UDF calls geopy at the worker level, so *sdf_dim_city* must already
    contain LAT/LON values (i.e. come from ``build_dim_city_augmented``).
    """
    geodesic_udf = F.udf(geodesic_km, DoubleType())

    sdf_pairs = (
        sdf_missions_raw.select(
            F.col("VILLE_DEPART").alias("ORIGIN"),
            F.col("VILLE_DESTINATION").alias("DESTINATION"),
        ).distinct()
    )

    sdf_origin = sdf_dim_city.select(
        F.col("SK_CITY").alias("SK_CITY_ORIGIN"),
        F.col("CITY_NAME").alias("ORIGIN"),
        F.col("LAT").alias("LAT_ORIGIN"),
        F.col("LON").alias("LON_ORIGIN"),
    )
    sdf_dest = sdf_dim_city.select(
        F.col("SK_CITY").alias("SK_CITY_DESTINATION"),
        F.col("CITY_NAME").alias("DESTINATION"),
        F.col("LAT").alias("LAT_DEST"),
        F.col("LON").alias("LON_DEST"),
    )

    return (
        sdf_pairs.join(sdf_origin, "ORIGIN", "left")
        .join(sdf_dest, "DESTINATION", "left")
        .withColumn(
            "DISTANCE_KM",
            geodesic_udf("LAT_ORIGIN", "LON_ORIGIN", "LAT_DEST", "LON_DEST"),
        )
        .withColumn(
            "SK_TRIP",
            F.row_number()
            .over(Window.orderBy("SK_CITY_ORIGIN", "SK_CITY_DESTINATION"))
            .cast("long"),
        )
        .select(DIM_TRIP_SCHEMA.fieldNames())
    )
