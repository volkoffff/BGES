from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from config.settings import ETLConfig
from etl.extract.readers import read_comma
from models.schemas import DIM_EQUIPMENT_SCHEMA


def build_dim_equipment(spark: SparkSession, config: ETLConfig) -> DataFrame:
    """Build DIM_EQUIPMENT from the CO2 reference CSV (Type, Modèle, Impact)."""
    sdf_raw = read_comma(spark, config.co2_ref_path())
    w = Window.orderBy(F.col("Type"), F.col("Modèle"))
    return sdf_raw.select(
        F.row_number().over(w).cast("long").alias("SK_EQUIPMENT"),
        F.col("Type").alias("TYPE"),
        F.col("Modèle").alias("MODEL"),
        F.col("Impact").cast("double").alias("CO2_IMPACT_KG_REF"),
    ).select(DIM_EQUIPMENT_SCHEMA.fieldNames())
