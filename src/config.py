import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv
from pyspark.sql import SparkSession


load_dotenv()


@dataclass(frozen=True)
class PipelineConfig:
    """Configuration shared across the retail feature pipeline."""

    data_dir: Path = Path(os.getenv("DATA_DIR", "data/sample"))
    output_dir: Path = Path(os.getenv("OUTPUT_DIR", "output"))
    spark_master: str = os.getenv("SPARK_MASTER", "local[*]")
    shuffle_partitions: int = int(
        os.getenv("SPARK_SHUFFLE_PARTITIONS", "8")
    )

    @property
    def transactions_path(self) -> Path:
        return self.data_dir / "transactions_train.csv"

    @property
    def customers_path(self) -> Path:
        return self.data_dir / "customers.csv"

    @property
    def articles_path(self) -> Path:
        return self.data_dir / "articles.csv"


def create_spark_session(config: PipelineConfig) -> SparkSession:
    """Create the Spark session used by the pipeline."""

    spark = (
        SparkSession.builder
        .appName("RetailFeaturePipeline")
        .master(config.spark_master)
        .config(
            "spark.sql.shuffle.partitions",
            str(config.shuffle_partitions),
        )
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    return spark