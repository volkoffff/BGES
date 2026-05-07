from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from models.schemas import FACT_MISSION_SCHEMA

# Normalisation multilingue de TYPE_MISSION vers 5 valeurs canoniques françaises.
_TYPE_MISSION_MAP = {
    # Français (Paris)
    "Conférence": "Conférence",
    "Développement": "Développement",
    "Formation": "Formation",
    "Rencontre entreprises": "Rencontre entreprises",
    "Réunion": "Réunion",
    # Allemand (Berlin)
    "Entwicklung": "Développement",
    "Geschäftstreffen": "Rencontre entreprises",
    "Konferenz": "Conférence",
    "Meeting": "Réunion",
    "Schulung": "Formation",
    # Anglais (London, New York, Los Angeles, Shanghai)
    "Business Meeting": "Rencontre entreprises",
    "Conference": "Conférence",
    "Development": "Développement",
    "Team Meeting": "Réunion",
    "Vocational Training": "Formation",
}


def build_fact_mission(
    spark: SparkSession,
    sdf_missions_raw: DataFrame,
    sdf_dim_date: DataFrame,
    sdf_dim_staff: DataFrame,
    sdf_dim_trip: DataFrame,
    sdf_dim_transport_type: DataFrame,
    sdf_dim_city: DataFrame,
) -> DataFrame:
    """Construire FACT_MISSION : CO2 = distance × facteur × (2 si aller-retour)."""
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

    # Résoudre le transport "Avion" en court-courrier / long-courrier selon la distance.
    sdf = (
        sdf_missions_raw.withColumn(
            "DATE_ISO", F.to_date(F.col("DATE_MISSION"), "yyyy-MM-dd HH:mm:ss")
        )
        .withColumn("ROUND_TRIP_FLG", F.col("ALLER_RETOUR") == F.lit("oui"))
        .withColumn("MISSION_TYPE", type_map_expr[F.col("TYPE_MISSION")])
        .join(
            sdf_dim_date.select("SK_DATE", "DATE_ISO"),
            "DATE_ISO",
            "left",
        )
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
        .withColumn(
            "SK_FACT_MISSION",
            F.row_number().over(Window.orderBy("NK_MISSION")).cast("long"),
        )
    )

    return sdf.select(FACT_MISSION_SCHEMA.fieldNames())
