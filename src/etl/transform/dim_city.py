"""Build DIM_CITY from org-site PERSONNEL files (initial) and mission cities (daily)."""

import logging
from pathlib import Path

import country_converter as coco
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StructField, StructType
from timezonefinder import TimezoneFinder

from config.settings import SITES, ETLConfig
from etl.extract.readers import read_semicolon_many
from models.schemas import DIM_CITY_SCHEMA, PERSONNEL_RAW_SCHEMA
from utils.geocoding import geocode_cities

# DIM_CITY_SCHEMA without LAT/LON; coordinates are needed internally to compute
# distances in DIM_TRIP but must not appear in the public dimension table.
_DIM_CITY_COORDS_SCHEMA = StructType(
    [
        *DIM_CITY_SCHEMA.fields,
        StructField("LAT", DoubleType(), nullable=True),
        StructField("LON", DoubleType(), nullable=True),
    ]
)

# country_converter logs a WARNING for every unrecognised name (e.g. French
# abbreviations like "Emirats").  Raise to ERROR so only hard failures surface.
logging.getLogger("country_converter").setLevel(logging.ERROR)

# Instantiated once at module level — the constructor loads a large reference CSV.
_CC = coco.CountryConverter()
_TF = TimezoneFinder()


def _country_name_to_iso2(country_name: str) -> str | None:
    """Retourne le code ISO-3166-1 alpha-2 pour country_name, ou None si inconnu.

    Args:
        country_name: Nom de pays tel qu'il apparaît dans les fichiers source.

    Returns:
        Code à deux lettres (ex. "FR"), ou None si country_converter ne résout pas.
    """
    iso2: str = _CC.convert(country_name, to="ISO2")
    return None if iso2 == "not found" else str(iso2)


def _pays_to_iso2_map(country_names: list[str]) -> dict[str, str | None]:
    """Mappe chaque nom de pays à son code ISO-3166-1 alpha-2.

    Args:
        country_names: Liste de noms de pays distincts à convertir.

    Returns:
        Dictionnaire nom → code ISO2, ou None pour les noms non résolus.
    """
    return {name: _country_name_to_iso2(name) for name in country_names}


def _lat_lon_to_timezone(lat: float | None, lon: float | None) -> str | None:
    """Retourne le fuseau horaire IANA pour des coordonnées, ou None si inconnu.

    Args:
        lat: Latitude en degrés décimaux.
        lon: Longitude en degrés décimaux.

    Returns:
        Identifiant IANA (ex. "Europe/Paris"), ou None si les coordonnées sont nulles
        ou si aucun fuseau n'est trouvé.
    """
    if lat is None or lon is None:
        return None
    return _TF.timezone_at(lat=lat, lng=lon)


def build_dim_city_initial(spark: SparkSession, config: ETLConfig) -> DataFrame:
    """Construit DIM_CITY pour les 6 sites org à partir des fichiers PERSONNEL.

    Args:
        spark: SparkSession active.
        config: Configuration ETL fournissant les chemins des fichiers PERSONNEL.

    Returns:
        DataFrame conforme à DIM_CITY_SCHEMA avec une ligne par ville distincte.
        IS_ORG_SITE est True pour chaque ligne. TIMEZONE_IANA est résolu via
        les coordonnées géocodées du cache.
    """
    paths: list[Path] = [config.personnel_path(s) for s in SITES]
    sdf_raw: DataFrame = read_semicolon_many(spark, paths, schema=PERSONNEL_RAW_SCHEMA)

    sdf_cities: DataFrame = sdf_raw.select(
        F.col("VILLE").alias("CITY_NAME"), F.col("PAYS")
    ).distinct()

    pays_values: list[str] = [
        row.PAYS for row in sdf_cities.select("PAYS").collect() if row.PAYS is not None
    ]
    iso2_map: dict[str, str | None] = _pays_to_iso2_map(pays_values)
    country_map_expr = F.create_map(
        *[x for kv in iso2_map.items() for x in (F.lit(kv[0]), F.lit(kv[1]))]
    )

    rows_raw = (
        sdf_cities.withColumn("COUNTRY_ISO2", country_map_expr[F.col("PAYS")])
        .withColumn("IS_ORG_SITE", F.lit(True))
        .withColumn("TIMEZONE_IANA", F.lit(None).cast("string"))
        .collect()
    )

    # Geocode org-site cities to resolve TIMEZONE_IANA from coordinates.
    city_names: list[str] = [r.CITY_NAME for r in rows_raw if r.CITY_NAME]
    coords: dict[str, tuple[float, float] | None] = geocode_cities(
        city_names, config.coordinates_path()
    )

    result: list[tuple] = []
    for sk, r in enumerate(
        sorted(rows_raw, key=lambda row: row.CITY_NAME or ""), start=1
    ):
        lat_lon = coords.get(r.CITY_NAME)
        tz: str | None = (
            _lat_lon_to_timezone(lat_lon[0], lat_lon[1]) if lat_lon else None
        )
        result.append((sk, r.CITY_NAME, r.COUNTRY_ISO2, r.IS_ORG_SITE, tz))

    return spark.createDataFrame(result, schema=DIM_CITY_SCHEMA)


def build_dim_city_augmented(
    spark: SparkSession,
    config: ETLConfig,
    sdf_missions_raw: DataFrame,
    sdf_dim_city_initial: DataFrame,
) -> DataFrame:
    """Étend DIM_CITY avec toutes les villes de mission géocodées via Nominatim.

    Les lignes des sites org (depuis sdf_dim_city_initial) conservent leurs SK_CITY
    et gagnent LAT/LON et TIMEZONE_IANA via le cache de géocodage. Les nouvelles
    villes sont ajoutées avec des SK_CITY consécutifs après le maximum initial.

    Args:
        spark: SparkSession active.
        config: Configuration ETL fournissant le chemin du cache de géocodage.
        sdf_missions_raw: DataFrame brut des missions avec VILLE_DEPART,
            PAYS_DEPART, VILLE_DESTINATION et PAYS_DESTINATION.
        sdf_dim_city_initial: DIM_CITY produit par build_dim_city_initial.

    Returns:
        DataFrame avec les colonnes de DIM_CITY_SCHEMA **plus LAT et LON**
        (usage interne uniquement — les appelants doivent supprimer ces colonnes
        avant de stocker la table finale).
    """
    initial_rows = sdf_dim_city_initial.collect()
    initial_city_names: set[str] = {r.CITY_NAME for r in initial_rows}
    max_sk: int = max(r.SK_CITY for r in initial_rows)

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

    new_city_pays: dict[str, str | None] = {}
    for r in mission_city_rows:
        if (
            r.CITY_NAME is not None
            and r.CITY_NAME not in initial_city_names
            and r.CITY_NAME not in new_city_pays
        ):
            new_city_pays[r.CITY_NAME] = r.PAYS

    all_city_names: list[str] = list(initial_city_names) + list(new_city_pays.keys())
    coords: dict[str, tuple[float, float] | None] = geocode_cities(
        all_city_names, config.coordinates_path()
    )

    pays_with_values: list[str] = [
        p for p in set(new_city_pays.values()) if p is not None
    ]
    iso2_map: dict[str, str | None] = (
        _pays_to_iso2_map(pays_with_values) if pays_with_values else {}
    )
    city_iso2: dict[str, str | None] = {
        city: iso2_map.get(pays) if pays else None
        for city, pays in new_city_pays.items()
    }

    result_rows: list[tuple] = []

    for r in initial_rows:
        lat_lon = coords.get(r.CITY_NAME)
        tz: str | None = (
            _lat_lon_to_timezone(lat_lon[0], lat_lon[1]) if lat_lon else None
        )
        result_rows.append(
            (
                r.SK_CITY,
                r.CITY_NAME,
                r.COUNTRY_ISO2,
                r.IS_ORG_SITE,
                tz,
                lat_lon[0] if lat_lon else None,
                lat_lon[1] if lat_lon else None,
            )
        )

    for i, city_name in enumerate(sorted(new_city_pays.keys()), start=max_sk + 1):
        lat_lon = coords.get(city_name)
        tz = _lat_lon_to_timezone(lat_lon[0], lat_lon[1]) if lat_lon else None
        result_rows.append(
            (
                i,
                city_name,
                city_iso2.get(city_name),
                False,
                tz,
                lat_lon[0] if lat_lon else None,
                lat_lon[1] if lat_lon else None,
            )
        )

    return spark.createDataFrame(result_rows, schema=_DIM_CITY_COORDS_SCHEMA)
