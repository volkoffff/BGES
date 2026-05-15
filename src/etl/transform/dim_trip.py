"""Build DIM_TRIP: distinct (origin, destination) city pairs with geodesic distance."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

from models.schemas import DIM_TRIP_SCHEMA


@F.udf(DoubleType())
def _geodesic_km(
    lat1: float | None,
    lon1: float | None,
    lat2: float | None,
    lon2: float | None,
) -> float | None:
    # Inline import so cloudpickle serialises bytecode only — no utils.distance
    # dependency at the Spark worker level (workers lack src/ on their sys.path).
    if any(v is None for v in (lat1, lon1, lat2, lon2)):
        return None
    from geopy.distance import geodesic

    return float(geodesic((lat1, lon1), (lat2, lon2)).km)


def build_dim_trip(
    sdf_missions_raw: DataFrame,
    sdf_dim_city: DataFrame,
) -> DataFrame:
    """Return one row per distinct (origin, destination) pair with DISTANCE_KM.

    The UDF calls geopy at the worker level, so *sdf_dim_city* must already
    contain LAT/LON values (i.e. come from ``build_dim_city_augmented``).

    Args:
        sdf_missions_raw: Raw missions DataFrame with VILLE_DEPART and
            VILLE_DESTINATION columns.
        sdf_dim_city: Augmented DIM_CITY DataFrame that must include LAT and
            LON columns for distance computation.

    Returns:
        DataFrame matching DIM_TRIP_SCHEMA with one row per distinct city pair
        and DISTANCE_KM computed via the geodesic UDF (null when coordinates
        are missing for either city).
    """

    sdf_pairs = sdf_missions_raw.select(
        F.col("VILLE_DEPART").alias("ORIGIN"),
        F.col("VILLE_DESTINATION").alias("DESTINATION"),
    ).distinct()

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
            _geodesic_km("LAT_ORIGIN", "LON_ORIGIN", "LAT_DEST", "LON_DEST"),
        )
        .withColumn("SK_TRIP", F.monotonically_increasing_id())
        .select(DIM_TRIP_SCHEMA.fieldNames())
    )
