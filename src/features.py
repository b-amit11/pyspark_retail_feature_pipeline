from dataclasses import dataclass

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.config import PipelineConfig, create_spark_session
from src.extract import extract_data
from src.transform import transform_data


@dataclass(frozen=True)
class FeatureTables:
    """ML-ready feature tables produced by the pipeline."""

    customer_features: DataFrame
    product_features: DataFrame
    transaction_features: DataFrame


def build_transaction_features(
    transactions: DataFrame,
) -> DataFrame:
    """Add sequential customer-purchase features."""

    customer_window = Window.partitionBy("customer_id").orderBy(
        "transaction_date",
        "article_id",
    )

    return (
        transactions
        .withColumn(
            "customer_purchase_number",
            F.row_number().over(customer_window),
        )
        .withColumn(
            "previous_purchase_date",
            F.lag("transaction_date").over(customer_window),
        )
        .withColumn(
            "days_since_previous_purchase",
            F.datediff(
                "transaction_date",
                "previous_purchase_date",
            ),
        )
    )


def build_customer_features(
    transactions: DataFrame,
    customers: DataFrame,
) -> DataFrame:
    """Aggregate transaction behavior into customer-level features."""

    aggregates = (
        transactions
        .groupBy("customer_id")
        .agg(
            F.count("*").alias("transaction_count"),
            F.countDistinct("article_id").alias("unique_articles"),
            F.round(F.sum("price"), 4).alias("total_spend"),
            F.round(F.avg("price"), 4).alias("average_price"),
            F.min("transaction_date").alias("first_purchase_date"),
            F.max("transaction_date").alias("last_purchase_date"),
            F.sum(
                F.when(F.col("sales_channel_id") == 1, 1).otherwise(0)
            ).alias("channel_1_transactions"),
            F.sum(
                F.when(F.col("sales_channel_id") == 2, 1).otherwise(0)
            ).alias("channel_2_transactions"),
        )
        .withColumn(
            "purchase_span_days",
            F.datediff(
                "last_purchase_date",
                "first_purchase_date",
            ),
        )
        .withColumn(
            "channel_2_ratio",
            F.round(
                F.col("channel_2_transactions")
                / F.col("transaction_count"),
                4,
            ),
        )
    )

    return (
        customers
        .join(aggregates, on="customer_id", how="left")
        .fillna(
            {
                "transaction_count": 0,
                "unique_articles": 0,
                "total_spend": 0.0,
                "average_price": 0.0,
                "channel_1_transactions": 0,
                "channel_2_transactions": 0,
                "purchase_span_days": 0,
                "channel_2_ratio": 0.0,
            }
        )
    )


def build_product_features(
    transactions: DataFrame,
    articles: DataFrame,
) -> DataFrame:
    """Aggregate purchasing behavior into product-level features."""

    aggregates = (
        transactions
        .groupBy("article_id")
        .agg(
            F.count("*").alias("purchase_count"),
            F.countDistinct("customer_id").alias("unique_customers"),
            F.round(F.avg("price"), 4).alias("average_selling_price"),
            F.round(F.sum("price"), 4).alias("total_revenue"),
            F.min("transaction_date").alias("first_purchase_date"),
            F.max("transaction_date").alias("last_purchase_date"),
        )
    )

    article_details = articles.select(
        "article_id",
        "prod_name",
        "product_type_name",
        "product_group_name",
        "colour_group_name",
        "department_name",
        "section_name",
        "garment_group_name",
    )

    return (
        article_details
        .join(aggregates, on="article_id", how="left")
        .fillna(
            {
                "purchase_count": 0,
                "unique_customers": 0,
                "average_selling_price": 0.0,
                "total_revenue": 0.0,
            }
        )
    )


def build_feature_tables(
    transactions: DataFrame,
    customers: DataFrame,
    articles: DataFrame,
) -> FeatureTables:
    """Build every ML-ready feature table."""

    return FeatureTables(
        customer_features=build_customer_features(
            transactions,
            customers,
        ),
        product_features=build_product_features(
            transactions,
            articles,
        ),
        transaction_features=build_transaction_features(
            transactions
        ),
    )


def main() -> None:
    """Build and display features from the local sample data."""

    config = PipelineConfig()
    spark = create_spark_session(config)

    try:
        raw = extract_data(spark, config)

        clean = transform_data(
            raw.transactions,
            raw.customers,
            raw.articles,
        )

        features = build_feature_tables(
            clean.transactions,
            clean.customers,
            clean.articles,
        )

        print("\nCustomer features:")
        features.customer_features.orderBy("customer_id").show(
            truncate=False
        )

        print("\nProduct features:")
        features.product_features.orderBy("article_id").show(
            truncate=False
        )

        print("\nTransaction window features:")
        features.transaction_features.orderBy(
            "customer_id",
            "customer_purchase_number",
        ).show(truncate=False)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()