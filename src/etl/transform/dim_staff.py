"""Build DIM_STAFF from PERSONNEL files with multilingual job-title normalisation."""

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from config.settings import SITES, ETLConfig
from etl.extract.readers import read_semicolon_many
from models.schemas import DIM_STAFF_SCHEMA, PERSONNEL_RAW_SCHEMA

# Maps all multilingual FONCTION_PERSONNEL values to 5 canonical French labels.
_FONCTION_MAP: dict[str, str] = {
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

# Maps each canonical JOB_TITLE to a broader ACTIVITY_SECTOR for BGES reporting.
_SECTOR_MAP: dict[str, str] = {
    "Cadre": "Management",
    "DRH": "Ressources Humaines",
    "Economiste": "Finance",
    "Ingénieur Data": "Informatique",
    "Ingénieur Informaticien": "Informatique",
}


def build_dim_staff(
    spark: SparkSession, config: ETLConfig, sdf_dim_city: DataFrame
) -> DataFrame:
    """Construit DIM_STAFF depuis les fichiers PERSONNEL avec normalisation multilingue.

    Args:
        spark: SparkSession active.
        config: Configuration ETL fournissant les chemins des fichiers PERSONNEL.
        sdf_dim_city: DIM_CITY utilisé pour résoudre le SK_SITE de chaque employé.

    Returns:
        DataFrame conforme à DIM_STAFF_SCHEMA avec JOB_TITLE normalisé en 5 labels
        canoniques français et ACTIVITY_SECTOR déduit du JOB_TITLE.
    """
    paths: list[Path] = [config.personnel_path(s) for s in SITES]
    sdf_raw: DataFrame = read_semicolon_many(spark, paths, schema=PERSONNEL_RAW_SCHEMA)

    fonction_map_expr = F.create_map(
        *[x for kv in _FONCTION_MAP.items() for x in (F.lit(kv[0]), F.lit(kv[1]))]
    )
    sector_map_expr = F.create_map(
        *[x for kv in _SECTOR_MAP.items() for x in (F.lit(kv[0]), F.lit(kv[1]))]
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
        .withColumn("ACTIVITY_SECTOR", sector_map_expr[F.col("JOB_TITLE")])
        .withColumn("SK_STAFF", F.monotonically_increasing_id())
        .select(DIM_STAFF_SCHEMA.fieldNames())
    )
