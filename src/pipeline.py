from src.config import PipelineConfig, create_spark_session
from src.extract import extract_data
from src.features import build_feature_tables
from src.load import load_feature_tables
from src.transform import transform_data


def run_pipeline() -> None:
    """Run the complete extract-transform-load feature pipeline."""

    config = PipelineConfig()
    spark = create_spark_session(config)

    try:
        print("\n1. Extracting raw datasets...")
        raw = extract_data(spark, config)

        print("2. Cleaning and validating data...")
        clean = transform_data(
            raw.transactions,
            raw.customers,
            raw.articles,
        )

        print("3. Building ML-ready feature tables...")
        features = build_feature_tables(
            clean.transactions,
            clean.customers,
            clean.articles,
        )

        print("4. Writing feature tables as Parquet...")
        load_feature_tables(
            features,
            config.output_dir,
        )

        print("\nPipeline completed successfully.")
        print(f"Output location: {config.output_dir.resolve()}")

    finally:
        spark.stop()


if __name__ == "__main__":
    run_pipeline()