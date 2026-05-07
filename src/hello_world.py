"""Hello World PySpark – vérifie que l'environnement fonctionne correctement."""

import os

os.environ["PYARROW_IGNORE_TIMEZONE"] = "1"

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main() -> None:
    spark = (
        SparkSession.builder.appName("BGES – Hello World")
        .master("local[*]")
        .config("spark.driver.host", "localhost")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    data = [
        ("Paris", "France", 2_161_000),
        ("Berlin", "Germany", 3_645_000),
        ("London", "UK", 8_982_000),
        ("New York", "USA", 8_336_000),
        ("Los Angeles", "USA", 3_979_000),
        ("Shanghai", "China", 24_870_000),
    ]

    sdf = spark.createDataFrame(data, ["city", "country", "population"])

    print("\n=== Villes du projet BGES ===")
    sdf.show()

    print("=== Population totale par pays ===")
    sdf.groupBy("country").agg(F.sum("population").alias("total_population")).orderBy(
        F.desc("total_population")
    ).show()

    java_version = spark.sparkContext._jvm.System.getProperty("java.version")  # type: ignore[union-attr]
    print(f"PySpark {spark.version}  |  Java {java_version}")

    spark.stop()


if __name__ == "__main__":
    main()
