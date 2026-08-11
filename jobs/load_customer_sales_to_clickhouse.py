from urllib.parse import urlencode
from urllib.request import Request, urlopen

from pyspark.sql.functions import (
    count,
    sum as spark_sum,
)

from spark_common import create_iceberg_spark


CLICKHOUSE_HTTP_URL = "http://clickhouse:8123"

CLICKHOUSE_JDBC_URL = (
    "jdbc:clickhouse://clickhouse:8123/analytics"
)

CLICKHOUSE_USER = "analytics"
CLICKHOUSE_PASSWORD = "analytics_pass"

CLICKHOUSE_TABLE = "customer_sales"


def truncate_customer_sales() -> None:
    params = urlencode(
        {
            "user": CLICKHOUSE_USER,
            "password": CLICKHOUSE_PASSWORD,
            "query": (
                "TRUNCATE TABLE "
                "analytics.customer_sales"
            ),
        }
    )

    url = f"{CLICKHOUSE_HTTP_URL}/?{params}"

    request = Request(
        url,
        method="POST",
    )

    with urlopen(
        request,
        timeout=30,
    ) as response:
        response.read()

    print(
        "ClickHouse table customer_sales truncated"
    )


def build_customer_sales(orders_df):
    return (
        orders_df
        .groupBy("customer_id")
        .agg(
            count("*").alias(
                "orders_count"
            ),
            spark_sum("quantity").alias(
                "total_quantity"
            ),
            spark_sum("amount").alias(
                "total_revenue"
            ),
        )
    )


def write_to_clickhouse(
    customer_sales_df,
) -> None:

    (
        customer_sales_df.write
        .format("jdbc")
        .option(
            "url",
            CLICKHOUSE_JDBC_URL,
        )
        .option(
            "dbtable",
            CLICKHOUSE_TABLE,
        )
        .option(
            "user",
            CLICKHOUSE_USER,
        )
        .option(
            "password",
            CLICKHOUSE_PASSWORD,
        )
        .option(
            "driver",
            "com.clickhouse.jdbc.ClickHouseDriver",
        )
        .mode("append")
        .save()
    )

    print(
        "customer_sales loaded into ClickHouse"
    )


def main() -> None:
    spark = create_iceberg_spark("LoadCustomerSalesToClickHouse")

    spark.sparkContext.setLogLevel("WARN")

    # ---------------------------------------------
    # EXTRACT
    # Iceberg -> Spark
    # ---------------------------------------------

    print("Reading orders from Iceberg...")

    orders_df = spark.table(
        "lake.sales.orders"
    )

    orders_df.show(
        truncate=False
    )

    orders_count = orders_df.count()

    print(
        f"Orders count: {orders_count}"
    )

    # ---------------------------------------------
    # TRANSFORM
    # ---------------------------------------------

    print(
        "Building customer_sales mart..."
    )

    customer_sales_df = (
        build_customer_sales(
            orders_df
        )
    )

    customer_sales_df.show(
        truncate=False
    )

    # ---------------------------------------------
    # LOAD
    #
    # Full refresh:
    #
    # 1. TRUNCATE старой витрины
    # 2. INSERT нового результата
    # ---------------------------------------------

    print(
        "Truncating ClickHouse mart..."
    )

    truncate_customer_sales()

    print(
        "Writing data to ClickHouse..."
    )

    write_to_clickhouse(
        customer_sales_df
    )

    print(
        "Pipeline finished successfully"
    )

    spark.stop()


if __name__ == "__main__":
    main()