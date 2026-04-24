from src.config.spark_config import create_spark_session


def main():
    spark = create_spark_session("test-bronze-direct-select")

    try:
        df = spark.sql("""
            SELECT
               *
            FROM silver.saleslt_customer
           
        """)

        df.show(20, truncate=True)
        print(f"Total rows: {df.count()}")
        df.printSchema()

    finally:
        spark.stop()


if __name__ == "__main__":
    main()