from src.config.spark_config import create_spark_session

spark = create_spark_session("test-rest-catalog")

spark.sql("CREATE NAMESPACE IF NOT EXISTS demo.bronze")

spark.sql("""
    CREATE TABLE IF NOT EXISTS demo.bronze.customers (
        id INT,
        name STRING,
        city STRING
    )
    USING iceberg
""")

spark.sql("""
    INSERT INTO demo.bronze.customers VALUES
    (1, 'Otavio', 'Ribeirao Preto'),
    (2, 'Maria', 'Sao Paulo')
""")

spark.sql("SELECT * FROM demo.bronze.customers").show(truncate=False)

spark.stop()