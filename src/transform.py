from dataclasses import dataclass

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.config import PipelineConfig, create_spark_session
from src.extract import extract_data


@dataclass(frozen=True)
class CleanRetailData:
    """Container for validated and cleaned retail datasets."""

    transactions: DataFrame
    customers: DataFrame
    articles: DataFrame


def clean_transactions(transactions: DataFrame) -> DataFrame:
    """Clean and validate transaction records."""

    return (
        transactions
        .withColumn("transaction_date", F.to_date("t_dat", "yyyy-MM-dd"))
        .drop("t_dat")
        .filter(F.col("transaction_date").isNotNull())
        .filter(F.col("customer_id").isNotNull())
        .filter(F.col("article_id").isNotNull())
        .filter(F.col("price") > 0)
        .filter(F.col("sales_channel_id").isin(1, 2))
        .dropDuplicates()
    )


def clean_customers(customers: DataFrame) -> DataFrame:
    """Clean customer attributes and standardize categories."""

    cleaned_age = F.when(
        F.col("age").between(16, 100),
        F.col("age"),
    ).otherwise(F.lit(None).cast("integer"))

    cleaned_news_frequency = (
        F.when(
            F.upper(F.trim(F.col("fashion_news_frequency")))
            == "REGULARLY",
            "REGULARLY",
        )
        .when(
            F.upper(F.trim(F.col("fashion_news_frequency")))
            == "MONTHLY",
            "MONTHLY",
        )
        .when(
            F.col("fashion_news_frequency").isNull()
            | (
                F.upper(F.trim(F.col("fashion_news_frequency")))
                == "NONE"
            ),
            "NEVER",
        )
        .otherwise("UNKNOWN")
    )

    return (
        customers
        .filter(F.col("customer_id").isNotNull())
        .dropDuplicates(["customer_id"])
        .withColumn("age", cleaned_age.cast("integer"))
        .withColumn(
            "fashion_news_frequency",
            cleaned_news_frequency,
        )
        .fillna(
            {
                "FN": 0,
                "Active": 0,
                "club_member_status": "UNKNOWN",
            }
        )
        .withColumn("FN", F.col("FN").cast("integer"))
        .withColumn("Active", F.col("Active").cast("integer"))
        .withColumnRenamed("FN", "fn")
        .withColumnRenamed("Active", "active")
    )


def clean_articles(articles: DataFrame) -> DataFrame:
    """Remove invalid articles and standardize missing descriptions."""

    return (
        articles
        .filter(F.col("article_id").isNotNull())
        .dropDuplicates(["article_id"])
        .fillna(
            {
                "prod_name": "Unknown Product",
                "product_type_name": "Unknown",
                "product_group_name": "Unknown",
                "colour_group_name": "Unknown",
                "department_name": "Unknown",
                "index_name": "Unknown",
                "section_name": "Unknown",
                "garment_group_name": "Unknown",
                "detail_desc": "Description unavailable",
            }
        )
    )


def transform_data(
    transactions: DataFrame,
    customers: DataFrame,
    articles: DataFrame,
) -> CleanRetailData:
    """Apply cleaning rules to all raw datasets."""

    return CleanRetailData(
        transactions=clean_transactions(transactions),
        customers=clean_customers(customers),
        articles=clean_articles(articles),
    )


def main() -> None:
    """Run and inspect the cleaning stage using local sample data."""

    config = PipelineConfig()
    spark = create_spark_session(config)

    try:
        raw = extract_data(spark, config)

        cleaned = transform_data(
            raw.transactions,
            raw.customers,
            raw.articles,
        )

        print("\nCleaning results")
        print(
            "Transactions:",
            raw.transactions.count(),
            "raw ->",
            cleaned.transactions.count(),
            "clean",
        )
        print(
            "Customers:",
            raw.customers.count(),
            "raw ->",
            cleaned.customers.count(),
            "clean",
        )
        print(
            "Articles:",
            raw.articles.count(),
            "raw ->",
            cleaned.articles.count(),
            "clean",
        )

        print("\nClean transactions:")
        cleaned.transactions.orderBy("transaction_date").show(
            truncate=False
        )

        print("\nClean customer ages:")
        cleaned.customers.select(
            "customer_id",
            "age",
            "fashion_news_frequency",
        ).orderBy("customer_id").show(truncate=False)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()