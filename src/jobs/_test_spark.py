from src.config.spark_config import create_spark_session

spark = create_spark_session("test-rest-catalog")

spark.sql("SELECT * FROM demo.bronze.saleslt_product where ProductID = 680").show(truncate=False)

spark.stop()