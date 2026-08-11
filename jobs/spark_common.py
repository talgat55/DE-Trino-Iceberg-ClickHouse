from pyspark.sql import SparkSession


def create_iceberg_spark(app_name: str) -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(
            "spark.sql.catalog.lake",
            "org.apache.iceberg.spark.SparkCatalog",
        )
        .config(
            "spark.sql.catalog.lake.catalog-impl",
            "org.apache.iceberg.jdbc.JdbcCatalog",
        )
        .config(
            "spark.sql.catalog.lake.uri",
            "jdbc:postgresql://airflow-db:5432/iceberg",
        )
        .config("spark.sql.catalog.lake.jdbc.user", "airflow")
        .config("spark.sql.catalog.lake.jdbc.password", "airflow")
        .config(
            "spark.sql.catalog.lake.warehouse",
            "s3a://warehouse/iceberg/",
        )
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem",
        )
        .getOrCreate()
    )
