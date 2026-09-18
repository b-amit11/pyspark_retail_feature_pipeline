from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.features import FeatureTables


def write_parquet(
    dataframe: DataFrame,
    output_path: Path,
) -> None:
    """Write a DataFrame as compressed Parquet files."""

    (
        dataframe.write
        .mode("overwrite")
        .option("compression", "snappy")
        .parquet(str(output_path))
    )


def write_transaction_features(
    transaction_features: DataFrame,
    output_path: Path,
) -> None:
    """Partition transaction features by year and month."""

    partitioned_transactions = (
        transaction_features
        .withColumn(
            "transaction_year",
            F.year("transaction_date"),
        )
        .withColumn(
            "transaction_month",
            F.month("transaction_date"),
        )
        .repartition(
            "transaction_year",
            "transaction_month",
        )
    )

    (
        partitioned_transactions.write
        .mode("overwrite")
        .option("compression", "snappy")
        .partitionBy(
            "transaction_year",
            "transaction_month",
        )
        .parquet(str(output_path))
    )


def load_feature_tables(
    features: FeatureTables,
    output_dir: Path,
) -> None:
    """Write all ML-ready feature tables to Parquet."""

    output_dir.mkdir(parents=True, exist_ok=True)

    write_parquet(
        features.customer_features,
        output_dir / "customer_features",
    )

    write_parquet(
        features.product_features,
        output_dir / "product_features",
    )

    write_transaction_features(
        features.transaction_features,
        output_dir / "transaction_features",
    )