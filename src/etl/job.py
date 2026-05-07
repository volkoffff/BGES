from abc import ABC, abstractmethod

from pyspark.sql import DataFrame, SparkSession

from config.settings import ETLConfig


class ETLJob(ABC):
    """Common contract for all ETL jobs: extract → transform → DataFrame."""

    def __init__(self, spark: SparkSession, config: ETLConfig) -> None:
        self.spark = spark
        self.config = config

    @abstractmethod
    def extract(self) -> DataFrame:
        """Read source files and return the raw DataFrame."""
        ...

    @abstractmethod
    def transform(self, sdf_raw: DataFrame) -> DataFrame:
        """Apply cleaning, normalization, and business logic."""
        ...

    def run(self) -> DataFrame:
        """Run the job and return the final DataFrame."""
        sdf_raw = self.extract()
        return self.transform(sdf_raw)
