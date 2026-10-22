import pytest

from src.features import (
    build_customer_features,
    build_feature_tables,
    build_product_features,
)


@pytest.fixture
def transactions(spark):
    """Three clean transactions across two customers and two articles."""

    rows = [
        ("C001", "A001", 0.05, 1, "2020-09-01"),
        ("C001", "A002", 0.10, 2, "2020-09-11"),
        ("C002", "A001", 0.05, 2, "2020-09-05"),
    ]

    columns = [
        "customer_id",
        "article_id",
        "price",
        "sales_channel_id",
        "transaction_date",
    ]

    return spark.createDataFrame(rows, columns).selectExpr(
        "customer_id",
        "article_id",
        "price",
        "sales_channel_id",
        "to_date(transaction_date) AS transaction_date",
    )


@pytest.fixture
def customers(spark):
    """C003 has no purchases and should still appear with zeroed features."""

    return spark.createDataFrame(
        [("C001", 24), ("C002", 31), ("C003", 45)],
        ["customer_id", "age"],
    )


@pytest.fixture
def articles(spark):
    """A003 has never been purchased."""

    columns = [
        "article_id",
        "prod_name",
        "product_type_name",
        "product_group_name",
        "colour_group_name",
        "department_name",
        "section_name",
        "garment_group_name",
    ]

    rows = [
        (article_id, f"Product {article_id}", "Top", "Garment", "Black",
         "Jersey", "Womens", "Jersey Basic")
        for article_id in ["A001", "A002", "A003"]
    ]

    return spark.createDataFrame(rows, columns)


def by_key(df, key):
    return {row[key]: row for row in df.collect()}


def test_customer_features_aggregate_behavior(transactions, customers):
    """Counts, spend, purchase span and channel mix are computed per customer."""

    features = by_key(
        build_customer_features(transactions, customers),
        "customer_id",
    )

    c001 = features["C001"]

    assert c001.transaction_count == 2
    assert c001.unique_articles == 2
    assert c001.total_spend == pytest.approx(0.15)
    assert c001.average_price == pytest.approx(0.075)
    assert c001.purchase_span_days == 10
    assert c001.channel_1_transactions == 1
    assert c001.channel_2_transactions == 1
    assert c001.channel_2_ratio == pytest.approx(0.5)

    c002 = features["C002"]

    assert c002.transaction_count == 1
    assert c002.purchase_span_days == 0
    assert c002.channel_2_ratio == pytest.approx(1.0)


def test_customer_features_keep_customers_without_purchases(
    transactions,
    customers,
):
    """The left join keeps every customer and fills missing aggregates with zero."""

    features = by_key(
        build_customer_features(transactions, customers),
        "customer_id",
    )

    assert set(features) == {"C001", "C002", "C003"}

    c003 = features["C003"]

    assert c003.age == 45
    assert c003.transaction_count == 0
    assert c003.unique_articles == 0
    assert c003.total_spend == 0.0
    assert c003.channel_2_ratio == 0.0
    assert c003.first_purchase_date is None


def test_product_features_aggregate_sales(transactions, articles):
    """Purchase counts, unique buyers and revenue are computed per article."""

    features = by_key(
        build_product_features(transactions, articles),
        "article_id",
    )

    a001 = features["A001"]

    assert a001.purchase_count == 2
    assert a001.unique_customers == 2
    assert a001.total_revenue == pytest.approx(0.10)
    assert a001.average_selling_price == pytest.approx(0.05)
    assert a001.product_type_name == "Top"

    a003 = features["A003"]

    assert a003.purchase_count == 0
    assert a003.unique_customers == 0
    assert a003.total_revenue == 0.0


def test_feature_tables_have_one_row_per_entity(
    transactions,
    customers,
    articles,
):
    """Feature tables keep entity grain: no duplicate keys after joins."""

    tables = build_feature_tables(transactions, customers, articles)

    customer_ids = [
        row.customer_id for row in tables.customer_features.collect()
    ]
    article_ids = [
        row.article_id for row in tables.product_features.collect()
    ]

    assert sorted(customer_ids) == ["C001", "C002", "C003"]
    assert sorted(article_ids) == ["A001", "A002", "A003"]
    assert tables.transaction_features.count() == transactions.count()
