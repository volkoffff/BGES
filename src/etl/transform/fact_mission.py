"""Build FACT_MISSION: CO2 impact per business trip across all sites."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from models.schemas import FACT_MISSION_SCHEMA

# Maps all source TYPE_MISSION values to 5 canonical French labels.
_TYPE_MISSION_MAP = {
    # French (Paris)
    "Conférence": "Conférence",
    "Développement": "Développement",
    "Formation": "Formation",
    "Rencontre entreprises": "Rencontre entreprises",
    "Réunion": "Réunion",
    # German (Berlin)
    "Entwicklung": "Développement",
    "Geschäftstreffen": "Rencontre entreprises",
    "Konferenz": "Conférence",
    "Meeting": "Réunion",
    "Schulung": "Formation",
    # English (London, New York, Los Angeles, Shanghai)
    "Business Meeting": "Rencontre entreprises",
    "Conference": "Conférence",
    "Development": "Développement",
    "Team Meeting": "Réunion",
    "Vocational Training": "Formation",
}


def build_fact_mission(
    sdf_missions_raw: DataFrame,
    sdf_dim_date: DataFrame,
    sdf_dim_staff: DataFrame,
    sdf_dim_trip: DataFrame,
    sdf_dim_transport_type: DataFrame,
    sdf_dim_city: DataFrame,
) -> DataFrame:
    """Return FACT_MISSION with CO2_IMPACT_KG = distance × factor × (2 if round-trip).

    "Avion" trips are split into short-haul (< 1 000 km) and long-haul (≥ 1 000 km)
    before joining DIM_TRANSPORT_TYPE so the correct ADEME factor is applied.

    Args:
        sdf_missions_raw: Raw missions DataFrame (MISSION_RAW_SCHEMA).
        sdf_dim_date: DIM_DATE dimension for date surrogate-key resolution.
        sdf_dim_staff: DIM_STAFF dimension for staff surrogate-key resolution.
        sdf_dim_trip: DIM_TRIP dimension providing SK_TRIP and DISTANCE_KM.
        sdf_dim_transport_type: DIM_TRANSPORT_TYPE providing CO2_FACTOR_KG_PER_KM.
        sdf_dim_city: DIM_CITY dimension used to resolve origin/destination SKs.

    Returns:
        DataFrame matching FACT_MISSION_SCHEMA with one row per mission and
        CO2_IMPACT_KG computed from distance, transport factor and trip direction.
    """
    type_map_expr = F.create_map(
        *[x for kv in _TYPE_MISSION_MAP.items() for x in (F.lit(kv[0]), F.lit(kv[1]))]
    )

    sdf_city_origin = sdf_dim_city.select(
        F.col("SK_CITY").alias("SK_CITY_ORIGIN"),
        F.col("CITY_NAME").alias("VILLE_DEPART"),
    )
    sdf_city_dest = sdf_dim_city.select(
        F.col("SK_CITY").alias("SK_CITY_DESTINATION"),
        F.col("CITY_NAME").alias("VILLE_DESTINATION"),
    )

    sdf = (
        sdf_missions_raw.withColumn(
            "DATE_ISO", F.to_date(F.col("DATE_MISSION"), "yyyy-MM-dd HH:mm:ss")
        )
        # Null-safe: coalesce so that a missing ALLER_RETOUR becomes False (one-way).
        .withColumn(
            "ROUND_TRIP_FLG",
            F.coalesce(F.col("ALLER_RETOUR") == F.lit("oui"), F.lit(False)),
        )
        .withColumn("MISSION_TYPE", type_map_expr[F.col("TYPE_MISSION")])
        .join(sdf_dim_date.select("SK_DATE", "DATE_ISO"), "DATE_ISO", "left")
        .join(
            sdf_dim_staff.select(
                F.col("SK_STAFF"), F.col("NK_STAFF").alias("ID_PERSONNEL")
            ),
            "ID_PERSONNEL",
            "left",
        )
        .join(sdf_city_origin, "VILLE_DEPART", "left")
        .join(sdf_city_dest, "VILLE_DESTINATION", "left")
        .join(
            sdf_dim_trip.select(
                "SK_TRIP", "SK_CITY_ORIGIN", "SK_CITY_DESTINATION", "DISTANCE_KM"
            ),
            ["SK_CITY_ORIGIN", "SK_CITY_DESTINATION"],
            "left",
        )
        # Resolve "Avion" to short-haul or long-haul based on DISTANCE_KM.
        .withColumn(
            "TRANSPORT_NAME",
            F.when(
                (F.col("TRANSPORT") == "Avion") & (F.col("DISTANCE_KM") < 1000),
                F.lit("Avion court-courrier"),
            )
            .when(
                (F.col("TRANSPORT") == "Avion") & (F.col("DISTANCE_KM") >= 1000),
                F.lit("Avion long-courrier"),
            )
            .otherwise(F.col("TRANSPORT")),
        )
        .join(
            sdf_dim_transport_type.select(
                "SK_TRANSPORT_TYPE", "TRANSPORT_NAME", "CO2_FACTOR_KG_PER_KM"
            ),
            "TRANSPORT_NAME",
            "left",
        )
        .withColumn(
            "CO2_IMPACT_KG",
            F.col("DISTANCE_KM")
            * F.col("CO2_FACTOR_KG_PER_KM")
            * F.when(F.col("ROUND_TRIP_FLG"), 2).otherwise(1),
        )
        .withColumnRenamed("ID_MISSION", "NK_MISSION")
        .withColumnRenamed("SK_DATE", "SK_DATE_MISSION")
        .withColumn("SK_FACT_MISSION", F.monotonically_increasing_id())
    )

    return sdf.select(FACT_MISSION_SCHEMA.fieldNames())
