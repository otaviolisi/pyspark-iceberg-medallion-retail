from src.config.spark_config import create_spark_session


def main():
    spark = create_spark_session("test-spark-iceberg")

    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.bronze")
    spark.sql("""
        CREATE TABLE IF NOT EXISTS local.bronze.customers (
            id INT,
            name STRING,
            city STRING
        )
        USING iceberg
    """)

    spark.sql("""
        INSERT INTO local.bronze.customers VALUES
        (1, 'Otavio', 'Ribeirao Preto'),
        (2, 'Jose', 'Sao Paulo')
    """)

    df = spark.sql("SELECT * FROM local.bronze.customers")
    df.show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()