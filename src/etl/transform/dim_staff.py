from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from config.settings import SITES, ETLConfig
from etl.extract.readers import read_semicolon_many
from models.schemas import DIM_STAFF_SCHEMA, PERSONNEL_RAW_SCHEMA

# Maps all multilingual FONCTION_PERSONNEL values to canonical French labels.
_FONCTION_MAP = {
    # French (Paris)
    "Cadre": "Cadre",
    "DRH": "DRH",
    "Economiste": "Economiste",
    "Ingénieur Data": "Ingénieur Data",
    "Ingénieur Informaticien": "Ingénieur Informaticien",
    # German (Berlin)
    "Führungskraft": "Cadre",
    "Personalleiter": "DRH",
    "Ökonom": "Economiste",
    "Dateningenieur": "Ingénieur Data",
    "Computeringenieur": "Ingénieur Informaticien",
    # English (London, New York, Los Angeles, Shanghai)
    "Business Executive": "Cadre",
    "HRD": "DRH",
    "Economist": "Economiste",
    "Data Engineer": "Ingénieur Data",
    "Computer Engineer": "Ingénieur Informaticien",
}


def build_dim_staff(
    spark: SparkSession, config: ETLConfig, sdf_dim_city: DataFrame
) -> DataFrame:
    """Build DIM_STAFF from all PERSONNEL files with multilingual job normalization."""
    paths = [config.personnel_path(s) for s in SITES]
    sdf_raw = read_semicolon_many(spark, paths, schema=PERSONNEL_RAW_SCHEMA)

    fonction_map_expr = F.create_map(
        *[x for kv in _FONCTION_MAP.items() for x in (F.lit(kv[0]), F.lit(kv[1]))]
    )

    return (
        sdf_raw.select(
            F.col("ID_PERSONNEL").alias("NK_STAFF"),
            F.col("NOM_PERSONNEL").alias("LAST_NAME"),
            F.col("PRENOM_PERSONNEL").alias("FIRST_NAME"),
            fonction_map_expr[F.col("FONCTION_PERSONNEL")].alias("JOB_TITLE"),
            F.to_date(F.col("DT_NAISS"), "yyyy-MM-dd").alias("BIRTH_DATE"),
            F.col("VILLE"),
        )
        .join(
            sdf_dim_city.select(F.col("SK_CITY").alias("SK_SITE"), "CITY_NAME"),
            F.col("VILLE") == F.col("CITY_NAME"),
        )
        .withColumn(
            "SK_STAFF",
            F.row_number().over(Window.orderBy("NK_STAFF")).cast("long"),
        )
        .select(DIM_STAFF_SCHEMA.fieldNames())
    )
