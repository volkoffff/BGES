"""Build DIM_CITY from org-site PERSONNEL files (initial) and mission cities (daily)."""

import logging

import country_converter as coco
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StructField, StructType

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


def _country_name_to_iso2(country_name: str) -> str | None:
    """Return the ISO-3166-1 alpha-2 code for *country_name*, or None if unknown.

    Args:
        country_name: Full country name as it appears in the source files.

    Returns:
        Two-letter ISO-3166-1 alpha-2 code (e.g. "FR"), or None when
        country_converter cannot resolve the name.
    """
    iso2 = _CC.convert(country_name, to="ISO2")
    return None if iso2 == "not found" else str(iso2)


def _pays_to_iso2_map(country_names: list[str]) -> dict[str, str | None]:
    """Map each country name to its ISO-3166-1 alpha-2 code via country_converter.

    Args:
        country_names: List of distinct country names to convert.

    Returns:
        Mapping of country name to ISO-3166-1 alpha-2 code, or None for names
        that country_converter cannot resolve.
    """
    return {name: _country_name_to_iso2(name) for name in country_names}


def build_dim_city_initial(spark: SparkSession, config: ETLConfig) -> DataFrame:
    """Build DIM_CITY for the 6 org sites extracted from PERSONNEL files.

    Args:
        spark: Active SparkSession.
        config: ETL configuration providing PERSONNEL file paths.

    Returns:
        DataFrame matching DIM_CITY_SCHEMA with one row per distinct city found
        in the PERSONNEL files. TIMEZONE_IANA is null at this stage;
        IS_ORG_SITE is True for every row.
    """
    paths = [config.personnel_path(s) for s in SITES]
    sdf_raw = read_semicolon_many(spark, paths, schema=PERSONNEL_RAW_SCHEMA)

    sdf_cities = sdf_raw.select(
        F.col("VILLE").alias("CITY_NAME"), F.col("PAYS")
    ).distinct()

    # Collect the small country list at the driver to build a Spark map expression.
    pays_values = [
        row.PAYS for row in sdf_cities.select("PAYS").collect() if row.PAYS is not None
    ]
    iso2_map = _pays_to_iso2_map(pays_values)
    country_map_expr = F.create_map(
        *[x for kv in iso2_map.items() for x in (F.lit(kv[0]), F.lit(kv[1]))]
    )

    rows_raw = (
        sdf_cities.withColumn("COUNTRY_ISO2", country_map_expr[F.col("PAYS")])
        .withColumn("IS_ORG_SITE", F.lit(True))
        .withColumn("TIMEZONE_IANA", F.lit(None).cast("string"))
        .collect()
    )
    result = [
        (sk, r.CITY_NAME, r.COUNTRY_ISO2, r.IS_ORG_SITE, r.TIMEZONE_IANA)
        for sk, r in enumerate(
            sorted(rows_raw, key=lambda r: r.CITY_NAME or ""), start=1
        )
    ]
    return spark.createDataFrame(result, schema=DIM_CITY_SCHEMA)


def build_dim_city_augmented(
    spark: SparkSession,
    config: ETLConfig,
    sdf_missions_raw: DataFrame,
    sdf_dim_city_initial: DataFrame,
) -> DataFrame:
    """Extend DIM_CITY with all mission cities, geocoded via Nominatim.

    Org-site rows (from *sdf_dim_city_initial*) keep their original SK_CITY values
    and gain LAT/LON from the geocoding cache.  New cities (mission origins and
    destinations not already present) are appended with consecutive SK_CITY values
    starting after the maximum initial SK.

    Args:
        spark: Active SparkSession used to create the final DataFrame.
        config: ETL configuration providing the geocoding cache path.
        sdf_missions_raw: Raw missions DataFrame containing VILLE_DEPART,
            PAYS_DEPART, VILLE_DESTINATION and PAYS_DESTINATION columns.
        sdf_dim_city_initial: DIM_CITY produced by build_dim_city_initial,
            used as the base set of already-known cities.

    Returns:
        DataFrame with the columns of DIM_CITY_SCHEMA **plus LAT and LON**
        (internal use only — callers must drop those columns before storing
        the final dimension table).  LAT/LON are needed by build_dim_trip to
        compute geodesic distances and must not be propagated further.
    """
    initial_rows = sdf_dim_city_initial.collect()
    initial_city_names = {r.CITY_NAME for r in initial_rows}
    max_sk = max(r.SK_CITY for r in initial_rows)

    # One collect for all mission city/country pairs (both legs).
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

    # Build {city_name: pays} for cities not yet in DIM_CITY (first PAYS wins).
    new_city_pays: dict[str, str | None] = {}
    for r in mission_city_rows:
        if (
            r.CITY_NAME is not None
            and r.CITY_NAME not in initial_city_names
            and r.CITY_NAME not in new_city_pays
        ):
            new_city_pays[r.CITY_NAME] = r.PAYS

    all_city_names = list(initial_city_names) + list(new_city_pays.keys())
    coords = geocode_cities(all_city_names, config.coordinates_path())

    pays_with_values = [p for p in set(new_city_pays.values()) if p is not None]
    iso2_map = _pays_to_iso2_map(pays_with_values) if pays_with_values else {}
    city_iso2: dict[str, str | None] = {
        city: iso2_map.get(pays) if pays else None
        for city, pays in new_city_pays.items()
    }

    result_rows: list[tuple] = []

    # Update initial org-site rows with geocoded coordinates.
    for r in initial_rows:
        lat_lon = coords.get(r.CITY_NAME)
        result_rows.append(
            (
                r.SK_CITY,
                r.CITY_NAME,
                r.COUNTRY_ISO2,
                r.IS_ORG_SITE,
                r.TIMEZONE_IANA,
                lat_lon[0] if lat_lon else None,
                lat_lon[1] if lat_lon else None,
            )
        )

    # Append new cities sorted deterministically to keep SK assignment stable.
    for i, city_name in enumerate(sorted(new_city_pays.keys()), start=max_sk + 1):
        lat_lon = coords.get(city_name)
        result_rows.append(
            (
                i,
                city_name,
                city_iso2.get(city_name),
                False,
                None,
                lat_lon[0] if lat_lon else None,
                lat_lon[1] if lat_lon else None,
            )
        )

    return spark.createDataFrame(result_rows, schema=_DIM_CITY_COORDS_SCHEMA)
