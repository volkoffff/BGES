"""Hello World PySpark – vérifie que l'environnement fonctionne correctement."""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main() -> None:
    spark = (
        SparkSession.builder
        .appName("BGES – Hello World")
        .master("local[*]")
        .config("spark.driver.host", "localhost")
        .config("spark.driver.bindAddress", "0.0.0.0")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    data = [
        ("Paris",      "France",  2_161_000),
        ("Berlin",     "Germany", 3_645_000),
        ("London",     "UK",      8_982_000),
        ("New York",   "USA",     8_336_000),
        ("Los Angeles","USA",     3_979_000),
        ("Shanghai",   "China",  24_870_000),
    ]
    columns = ["city", "country", "population"]

    df = spark.createDataFrame(data, columns)

    print("\n=== Villes du projet BGES ===")
    df.show()

    print("=== Population totale par pays ===")
    df.groupBy("country") \
      .agg(F.sum("population").alias("total_population")) \
      .orderBy(F.desc("total_population")) \
      .show()

    version = spark.version
    java_version = spark.sparkContext._jvm.System.getProperty("java.version")  # type: ignore[union-attr]
    print(f"PySpark {version}  |  Java {java_version}")

    spark.stop()


if __name__ == "__main__":
    main()
