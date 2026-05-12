"""Build DIM_EQUIPMENT from the equipment CO2 reference CSV."""

from pyspark.sql import DataFrame, SparkSession

from config.settings import ETLConfig
from etl.extract.readers import read_comma
from models.schemas import DIM_EQUIPMENT_SCHEMA


def build_dim_equipment(spark: SparkSession, config: ETLConfig) -> DataFrame:
    """Build DIM_EQUIPMENT from the CO2 reference CSV (Type, Modèle, Impact).

    Args:
        spark: Active SparkSession.
        config: ETL configuration providing the CO2 reference file path.

    Returns:
        DataFrame matching DIM_EQUIPMENT_SCHEMA with SK_EQUIPMENT, TYPE,
        MODEL and CO2_IMPACT_KG_REF columns ordered by (TYPE, MODEL).
    """
    rows_raw = read_comma(spark, config.co2_ref_path()).collect()
    result = [
        (
            sk,
            r["Type"],
            r["Modèle"],
            float(r["Impact"]) if r["Impact"] is not None else None,
        )
        for sk, r in enumerate(
            sorted(rows_raw, key=lambda r: (r["Type"] or "", r["Modèle"] or "")),
            start=1,
        )
    ]
    return spark.createDataFrame(result, schema=DIM_EQUIPMENT_SCHEMA)
