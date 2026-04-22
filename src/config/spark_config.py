from pyspark.sql import SparkSession
import sys
import os


def create_spark_session(app_name: str = "medallion-lakehouse") -> SparkSession:
    python_executable = sys.executable
    os.environ["PYSPARK_PYTHON"] = python_executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = python_executable

    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config(
            "spark.jars.packages",
            ",".join([
                "org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.1",
                "org.apache.hadoop:hadoop-aws:3.3.4",
                "com.microsoft.sqlserver:mssql-jdbc:12.6.3.jre11"
            ])
        )
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
        )
        .config("spark.sql.catalog.demo", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.demo.type", "rest")
        .config("spark.sql.catalog.demo.uri", "http://rest:8181")
        .config("spark.sql.catalog.demo.warehouse", "s3://warehouse/")
        .config("spark.sql.catalog.demo.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.demo.s3.endpoint", "http://minio:9000")
        .config("spark.sql.catalog.demo.s3.path-style-access", "true")
        .config("spark.sql.catalog.demo.s3.access-key-id", "admin")
        .config("spark.sql.catalog.demo.s3.secret-access-key", "password")
        .config("spark.sql.catalog.demo.s3.region", "us-east-1")
        .getOrCreate()
)
    spark.sparkContext.setLogLevel("WARN")
    return spark