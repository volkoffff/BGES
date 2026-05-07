import country_converter as coco
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from config.settings import SITES, ETLConfig
from etl.extract.readers import read_semicolon_many
from models.schemas import DIM_CITY_SCHEMA, PERSONNEL_RAW_SCHEMA
from utils.geocoding import geocode_cities


def _pays_to_iso2_map(pays_values: list[str]) -> dict[str, str | None]:
    """Convertir des noms de pays en codes ISO-3166-1 alpha-2 via country_converter."""
    cc = coco.CountryConverter()
    result: dict[str, str | None] = {}
    for pays in pays_values:
        iso2 = cc.convert(pays, to="ISO2")
        result[pays] = None if iso2 == "not found" else str(iso2)
    return result


def build_dim_city_initial(spark: SparkSession, config: ETLConfig) -> DataFrame:
    """Construire DIM_CITY pour les 6 sites organisationnels (fichiers PERSONNEL)."""
    paths = [config.personnel_path(s) for s in SITES]
    sdf_raw = read_semicolon_many(spark, paths, schema=PERSONNEL_RAW_SCHEMA)

    sdf_cities = (
        sdf_raw.select(F.col("VILLE").alias("CITY_NAME"), F.col("PAYS")).distinct()
    )

    pays_values = [row.PAYS for row in sdf_cities.select("PAYS").collect()]
    iso2_map = _pays_to_iso2_map(pays_values)
    country_map_expr = F.create_map(
        *[x for kv in iso2_map.items() for x in (F.lit(kv[0]), F.lit(kv[1]))]
    )

    return (
        sdf_cities.withColumn("COUNTRY_ISO2", country_map_expr[F.col("PAYS")])
        .withColumn("IS_ORG_SITE", F.lit(True))
        .withColumn("TIMEZONE_IANA", F.lit(None).cast("string"))
        .withColumn("LAT", F.lit(None).cast("double"))
        .withColumn("LON", F.lit(None).cast("double"))
        .withColumn(
            "SK_CITY",
            F.row_number().over(Window.orderBy("CITY_NAME")).cast("long"),
        )
        .select(DIM_CITY_SCHEMA.fieldNames())
    )


def build_dim_city_augmented(
    spark: SparkSession,
    config: ETLConfig,
    sdf_missions_raw: DataFrame,
    sdf_dim_city_initial: DataFrame,
) -> DataFrame:
    """Enrichir DIM_CITY avec toutes les villes des missions géocodées via Nominatim."""
    initial_rows = sdf_dim_city_initial.collect()
    initial_city_names = {r.CITY_NAME for r in initial_rows}
    max_sk = max(r.SK_CITY for r in initial_rows)

    mission_city_rows = (
        sdf_missions_raw.select(
            F.col("VILLE_DEPART").alias("CITY_NAME"),
            F.col("PAYS_DEPART").alias("PAYS"),
        )
        .union(
            sdf_missions_raw.select(
                F.col("VILLE_DESTINATION").alias("CITY_NAME"),
                F.col("PAYS_DESTINATION").alias("PAYS"),
            )
        )
        .distinct()
        .collect()
    )

    new_city_pays: dict[str, str] = {}
    for r in mission_city_rows:
        if r.CITY_NAME not in initial_city_names and r.CITY_NAME not in new_city_pays:
            new_city_pays[r.CITY_NAME] = r.PAYS

    all_city_names = list(initial_city_names) + list(new_city_pays.keys())
    coords = geocode_cities(all_city_names, config.coordinates_path())

    iso2_map = (
        _pays_to_iso2_map(list(set(new_city_pays.values()))) if new_city_pays else {}
    )
    city_iso2: dict[str, str | None] = {
        city: iso2_map.get(pays) for city, pays in new_city_pays.items()
    }

    result_rows: list[tuple] = []
    for r in initial_rows:
        lat_lon = coords.get(r.CITY_NAME)
        result_rows.append((
            r.SK_CITY,
            r.CITY_NAME,
            r.COUNTRY_ISO2,
            r.IS_ORG_SITE,
            r.TIMEZONE_IANA,
            lat_lon[0] if lat_lon else None,
            lat_lon[1] if lat_lon else None,
        ))

    for i, city_name in enumerate(sorted(new_city_pays.keys()), start=max_sk + 1):
        lat_lon = coords.get(city_name)
        result_rows.append((
            i,
            city_name,
            city_iso2.get(city_name),
            False,
            None,
            lat_lon[0] if lat_lon else None,
            lat_lon[1] if lat_lon else None,
        ))

    return spark.createDataFrame(result_rows, schema=DIM_CITY_SCHEMA)
