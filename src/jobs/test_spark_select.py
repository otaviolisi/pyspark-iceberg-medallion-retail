from src.config.spark_config import create_spark_session


def main():
    spark = create_spark_session("test-spark-select")

    print("\n=== TESTANDO SELECT DIRETO ===")

    df = spark.sql("""
        SELECT *
        FROM local.bronze.customers
    """)

    df.show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()