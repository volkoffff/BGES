"""Build FACT_EQUIPMENT: CO2 impact per IT equipment purchase across all sites."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from models.schemas import FACT_EQUIPMENT_SCHEMA

# Fallback model name used in the CO2 reference table when the exact model is absent.
_DEFAULT_MODEL = "modèle par défaut"


def build_fact_equipment(
    sdf_equipment_raw: DataFrame,
    sdf_dim_date: DataFrame,
    sdf_dim_staff: DataFrame,
    sdf_dim_equipment: DataFrame,
) -> DataFrame:
    """Return FACT_EQUIPMENT with CO2_IMPACT_KG looked up from DIM_EQUIPMENT.

    Lookup strategy (first match wins):
    1. Exact match on (TYPE, MODELE) after trimming raw whitespace.
    2. Fallback to the (TYPE, 'modèle par défaut') entry for the same type.
    CO2_IMPACT_KG is null only when neither match succeeds (unknown type).

    Args:
        sdf_equipment_raw: Raw equipment DataFrame (EQUIPMENT_RAW_SCHEMA).
        sdf_dim_date: DIM_DATE dimension for purchase-date surrogate-key resolution.
        sdf_dim_staff: DIM_STAFF dimension for staff surrogate-key resolution.
        sdf_dim_equipment: DIM_EQUIPMENT dimension used for CO2 impact lookup.

    Returns:
        DataFrame matching FACT_EQUIPMENT_SCHEMA with one row per purchase and
        CO2_IMPACT_KG resolved via exact or fallback model match.
    """
    sdf_exact = sdf_dim_equipment.select(
        F.col("SK_EQUIPMENT").alias("SK_EQUIPMENT_EXACT"),
        F.col("CO2_IMPACT_KG_REF").alias("CO2_EXACT"),
        F.col("TYPE").alias("TYPE_EXACT"),
        F.col("MODEL").alias("MODELE_EXACT"),
    )
    sdf_fallback = sdf_dim_equipment.filter(
        F.col("MODEL") == F.lit(_DEFAULT_MODEL)
    ).select(
        F.col("SK_EQUIPMENT").alias("SK_EQUIPMENT_FB"),
        F.col("CO2_IMPACT_KG_REF").alias("CO2_FB"),
        F.col("TYPE").alias("TYPE_FB"),
    )

    return (
        sdf_equipment_raw.withColumn(
            "DATE_ISO", F.to_date(F.col("DATE_ACHAT"), "yyyy-MM-dd HH:mm:ss")
        )
        .join(sdf_dim_date.select("SK_DATE", "DATE_ISO"), "DATE_ISO", "left")
        .join(
            sdf_dim_staff.select(
                F.col("SK_STAFF"), F.col("NK_STAFF").alias("ID_PERSONNEL")
            ),
            "ID_PERSONNEL",
            "left",
        )
        # Trim raw MODELE to handle trailing whitespace seen in source files.
        .join(
            sdf_exact,
            (F.col("TYPE") == F.col("TYPE_EXACT"))
            & (F.trim(F.col("MODELE")) == F.col("MODELE_EXACT")),
            "left",
        )
        # Fallback: use the default-model entry for the same equipment type.
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
        .withColumn("SK_FACT_EQUIPMENT", F.monotonically_increasing_id())
        .select(FACT_EQUIPMENT_SCHEMA.fieldNames())
    )
