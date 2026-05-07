from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from models.schemas import FACT_EQUIPMENT_SCHEMA

_DEFAULT_MODEL = "modèle par défaut"


def build_fact_equipment(
    spark: SparkSession,
    sdf_equipment_raw: DataFrame,
    sdf_dim_date: DataFrame,
    sdf_dim_staff: DataFrame,
    sdf_dim_equipment: DataFrame,
) -> DataFrame:
    """Construire FACT_EQUIPMENT : lookup CO2 par (TYPE, MODELE), fallback défaut."""
    sdf_exact = sdf_dim_equipment.select(
        F.col("SK_EQUIPMENT").alias("SK_EQUIPMENT_EXACT"),
        F.col("CO2_IMPACT_KG_REF").alias("CO2_EXACT"),
        F.col("TYPE").alias("TYPE_EXACT"),
        F.col("MODEL").alias("MODELE_EXACT"),
    )
    sdf_fallback = (
        sdf_dim_equipment.filter(F.col("MODEL") == F.lit(_DEFAULT_MODEL))
        .select(
            F.col("SK_EQUIPMENT").alias("SK_EQUIPMENT_FB"),
            F.col("CO2_IMPACT_KG_REF").alias("CO2_FB"),
            F.col("TYPE").alias("TYPE_FB"),
        )
    )

    return (
        sdf_equipment_raw.withColumn(
            "DATE_ISO", F.to_date(F.col("DATE_ACHAT"), "yyyy-MM-dd HH:mm:ss")
        )
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
        # Lookup exact (TYPE, MODELE)
        .join(
            sdf_exact,
            (F.col("TYPE") == F.col("TYPE_EXACT"))
            & (F.trim(F.col("MODELE")) == F.col("MODELE_EXACT")),
            "left",
        )
        # Fallback par type avec modèle par défaut
        .join(sdf_fallback, F.col("TYPE") == F.col("TYPE_FB"), "left")
        .withColumn(
            "SK_EQUIPMENT",
            F.coalesce("SK_EQUIPMENT_EXACT", "SK_EQUIPMENT_FB").cast("long"),
        )
        .withColumn(
            "CO2_IMPACT_KG",
            F.coalesce(F.col("CO2_EXACT"), F.col("CO2_FB")),
        )
        .withColumnRenamed("ID_MATERIELINFO", "NK_EQUIPMENT")
        .withColumnRenamed("SK_DATE", "SK_DATE_PURCHASE")
        .withColumn(
            "SK_FACT_EQUIPMENT",
            F.row_number().over(Window.orderBy("NK_EQUIPMENT")).cast("long"),
        )
        .select(FACT_EQUIPMENT_SCHEMA.fieldNames())
    )
