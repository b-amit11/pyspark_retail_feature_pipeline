from dataclasses import dataclass
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

from src.config import PipelineConfig, create_spark_session
from src.schemas import (
    ARTICLES_SCHEMA,
    CUSTOMERS_SCHEMA,
    TRANSACTIONS_SCHEMA,
)


@dataclass(frozen=True)
class RawRetailData:
    """Container for the three raw retail datasets."""

    transactions: DataFrame
    customers: DataFrame
    articles: DataFrame


def validate_input_paths(config: PipelineConfig) -> None:
    """Confirm that every required input file exists."""

    required_files = [
        config.transactions_path,
        config.customers_path,
        config.articles_path,
    ]

    missing_files = [
        path for path in required_files if not Path(path).is_file()
    ]

    if missing_files:
        missing_list = "\n".join(str(path) for path in missing_files)

        raise FileNotFoundError(
            f"Required input files were not found:\n{missing_list}"
        )


def read_csv(
    spark: SparkSession,
    path: Path,
    schema: StructType,
) -> DataFrame:
    """Read a CSV file using an explicit schema."""

    return (
        spark.read
        .option("header", "true")
        .option("mode", "FAILFAST")
        .schema(schema)
        .csv(str(path))
    )


def extract_data(
    spark: SparkSession,
    config: PipelineConfig,
) -> RawRetailData:
    """Load the raw transaction, customer, and article datasets."""

    validate_input_paths(config)

    transactions = read_csv(
        spark,
        config.transactions_path,
        TRANSACTIONS_SCHEMA,
    )

    customers = read_csv(
        spark,
        config.customers_path,
        CUSTOMERS_SCHEMA,
    )

    articles = read_csv(
        spark,
        config.articles_path,
        ARTICLES_SCHEMA,
    )

    return RawRetailData(
        transactions=transactions,
        customers=customers,
        articles=articles,
    )


def main() -> None:
    """Load the sample data and print basic extraction results."""

    config = PipelineConfig()
    spark = create_spark_session(config)

    try:
        raw_data = extract_data(spark, config)

        print("\nExtraction successful")
        print("Transactions:", raw_data.transactions.count())
        print("Customers:", raw_data.customers.count())
        print("Articles:", raw_data.articles.count())

        print("\nTransaction schema:")
        raw_data.transactions.printSchema()

        print("Transaction preview:")
        raw_data.transactions.show(5, truncate=False)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()