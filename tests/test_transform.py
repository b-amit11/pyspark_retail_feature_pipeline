from src.features import build_transaction_features
from src.transform import clean_customers, clean_transactions


def test_clean_transactions_removes_bad_rows(spark):
    """Duplicate and negative-price transactions should be removed."""

    rows = [
        ("2020-09-01", "C001", "A001", 0.05, 1),
        ("2020-09-01", "C001", "A001", 0.05, 1),
        ("2020-09-02", "C002", "A002", -0.01, 2),
    ]

    columns = [
        "t_dat",
        "customer_id",
        "article_id",
        "price",
        "sales_channel_id",
    ]

    raw = spark.createDataFrame(rows, columns)
    cleaned = clean_transactions(raw)

    assert cleaned.count() == 1

    result = cleaned.first()

    assert result.customer_id == "C001"
    assert result.article_id == "A001"
    assert result.price == 0.05


def test_clean_customers_rejects_invalid_age(spark):
    """Unrealistic customer ages should become null."""

    rows = [
        ("C001", 1, 1, "ACTIVE", "Regularly", 24, "P001"),
        ("C002", 0, 0, "ACTIVE", "NONE", 120, "P002"),
    ]

    columns = [
        "customer_id",
        "FN",
        "Active",
        "club_member_status",
        "fashion_news_frequency",
        "age",
        "postal_code",
    ]

    raw = spark.createDataFrame(rows, columns)
    cleaned = clean_customers(raw)

    invalid_customer = (
        cleaned
        .filter("customer_id = 'C002'")
        .first()
    )

    assert invalid_customer.age is None
    assert invalid_customer.fashion_news_frequency == "NEVER"


def test_transaction_features_create_purchase_sequence(spark):
    """Window logic should number purchases in chronological order."""

    rows = [
        ("C001", "A002", 0.07, 2, "2020-09-03"),
        ("C001", "A001", 0.05, 1, "2020-09-01"),
    ]

    columns = [
        "customer_id",
        "article_id",
        "price",
        "sales_channel_id",
        "transaction_date",
    ]

    transactions = (
        spark.createDataFrame(rows, columns)
        .selectExpr(
            "customer_id",
            "article_id",
            "price",
            "sales_channel_id",
            "to_date(transaction_date) AS transaction_date",
        )
    )

    featured = (
        build_transaction_features(transactions)
        .orderBy("customer_purchase_number")
        .collect()
    )

    assert featured[0].customer_purchase_number == 1
    assert featured[0].previous_purchase_date is None
    assert featured[1].customer_purchase_number == 2
    assert featured[1].days_since_previous_purchase == 2