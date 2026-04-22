from src.config.spark_config import create_spark_session
from src.config.settings import (
    SQLSERVER_HOST,
    SQLSERVER_PORT,
    SQLSERVER_DATABASE,
    SQLSERVER_USER,
    SQLSERVER_PASSWORD,
    SQLSERVER_ENCRYPT,
    SQLSERVER_TRUST_SERVER_CERTIFICATE,
)


def build_jdbc_url() -> str:
    return (
        f"jdbc:sqlserver://{SQLSERVER_HOST}:{SQLSERVER_PORT};"
        f"databaseName={SQLSERVER_DATABASE};"
        f"encrypt={SQLSERVER_ENCRYPT};"
        f"trustServerCertificate={SQLSERVER_TRUST_SERVER_CERTIFICATE};"
    )


def main():
    spark = create_spark_session("test-sqlserver-connection")

    jdbc_url = build_jdbc_url()

    query = """
    (
        SELECT 1 AS test_connection
    ) AS t
    """

    try:
        df = (
            spark.read.format("jdbc")
            .option("url", jdbc_url)
            .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
            .option("dbtable", query)
            .option("user", SQLSERVER_USER)
            .option("password", SQLSERVER_PASSWORD)
            .load()
        )

        print("Conexão criada. Testando leitura real...")
        df.show(truncate=False)
        print("Conexão com SQL Server OK.")

    except Exception as e:
        print("Falha ao conectar/ler do SQL Server.")
        print(type(e).__name__)
        print(str(e))
        raise

    finally:
        spark.stop()


if __name__ == "__main__":
    main()