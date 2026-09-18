from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


TRANSACTIONS_SCHEMA = StructType(
    [
        StructField("t_dat", StringType(), nullable=False),
        StructField("customer_id", StringType(), nullable=False),
        StructField("article_id", StringType(), nullable=False),
        StructField("price", DoubleType(), nullable=True),
        StructField("sales_channel_id", IntegerType(), nullable=True),
    ]
)


CUSTOMERS_SCHEMA = StructType(
    [
        StructField("customer_id", StringType(), nullable=False),
        StructField("FN", IntegerType(), nullable=True),
        StructField("Active", IntegerType(), nullable=True),
        StructField("club_member_status", StringType(), nullable=True),
        StructField("fashion_news_frequency", StringType(), nullable=True),
        StructField("age", IntegerType(), nullable=True),
        StructField("postal_code", StringType(), nullable=True),
    ]
)


ARTICLES_SCHEMA = StructType(
    [
        StructField("article_id", StringType(), nullable=False),
        StructField("product_code", StringType(), nullable=True),
        StructField("prod_name", StringType(), nullable=True),
        StructField("product_type_no", IntegerType(), nullable=True),
        StructField("product_type_name", StringType(), nullable=True),
        StructField("product_group_name", StringType(), nullable=True),
        StructField("graphical_appearance_no", IntegerType(), nullable=True),
        StructField("graphical_appearance_name", StringType(), nullable=True),
        StructField("colour_group_code", IntegerType(), nullable=True),
        StructField("colour_group_name", StringType(), nullable=True),
        StructField("perceived_colour_value_id", IntegerType(), nullable=True),
        StructField("perceived_colour_value_name", StringType(), nullable=True),
        StructField("perceived_colour_master_id", IntegerType(), nullable=True),
        StructField("perceived_colour_master_name", StringType(), nullable=True),
        StructField("department_no", IntegerType(), nullable=True),
        StructField("department_name", StringType(), nullable=True),
        StructField("index_code", StringType(), nullable=True),
        StructField("index_name", StringType(), nullable=True),
        StructField("index_group_no", IntegerType(), nullable=True),
        StructField("index_group_name", StringType(), nullable=True),
        StructField("section_no", IntegerType(), nullable=True),
        StructField("section_name", StringType(), nullable=True),
        StructField("garment_group_no", IntegerType(), nullable=True),
        StructField("garment_group_name", StringType(), nullable=True),
        StructField("detail_desc", StringType(), nullable=True),
    ]
)