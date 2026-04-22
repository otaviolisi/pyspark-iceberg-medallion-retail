from src.config.spark_config import create_spark_session


def main():
    spark = create_spark_session("test-bronze-direct-select")

    try:
        df = spark.sql("""
            SELECT
                ProductID,
                Name,
                ProductNumber,
                ModifiedDate,
                DiscontinuedDate,
                bronze_ingestion_timestamp,
                bronze_batch_id,
                bronze_load_strategy
            FROM local.bronze.saleslt_product
            ORDER BY bronze_ingestion_timestamp DESC
        """)

        df.show(20, truncate=False)
        print(f"Total rows: {df.count()}")
        df.printSchema()

    finally:
        spark.stop()


if __name__ == "__main__":
    main()