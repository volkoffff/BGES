import country_converter as coco
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from config.settings import SITES, ETLConfig
from etl.extract.readers import read_semicolon_many
from models.schemas import DIM_CITY_SCHEMA, PERSONNEL_RAW_SCHEMA


def _pays_to_iso2_map(pays_values: list[str]) -> dict[str, str | None]:
    """Map country name strings to ISO-3166-1 alpha-2 codes via country_converter."""
    cc = coco.CountryConverter()
    result: dict[str, str | None] = {}
    for pays in pays_values:
        iso2 = cc.convert(pays, to="ISO2")
        result[pays] = None if iso2 == "not found" else str(iso2)
    return result


def build_dim_city_initial(spark: SparkSession, config: ETLConfig) -> DataFrame:
    """Build DIM_CITY from the 6 org-site cities extracted from PERSONNEL files."""
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
