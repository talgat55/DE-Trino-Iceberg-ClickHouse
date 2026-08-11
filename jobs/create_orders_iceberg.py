from spark_common import create_iceberg_spark


def main() -> None:
    spark = create_iceberg_spark("CreateOrdersIceberg")
    spark.sparkContext.setLogLevel("WARN")

    spark.sql("""
            CREATE NAMESPACE IF NOT EXISTS lake.sales
        """)

    spark.sql("""
            CREATE TABLE IF NOT EXISTS lake.sales.orders (
                order_id BIGINT,
                customer_id BIGINT,
                product_id BIGINT,
                quantity INTEGER,
                amount DECIMAL(12, 2),
                status STRING,
                created_at TIMESTAMP
            )
            USING iceberg
            PARTITIONED BY (days(created_at))
        """)

    spark.sql("""
            INSERT INTO lake.sales.orders VALUES
                (
                    1,
                    101,
                    501,
                    2,
                    CAST(1500.00 AS DECIMAL(12, 2)),
                    'paid',
                    TIMESTAMP '2026-08-10 10:00:00'
                ),
                (
                    2,
                    102,
                    502,
                    1,
                    CAST(2700.00 AS DECIMAL(12, 2)),
                    'shipped',
                    TIMESTAMP '2026-08-10 11:00:00'
                ),
                (
                    3,
                    103,
                    501,
                    3,
                    CAST(900.00 AS DECIMAL(12, 2)),
                    'created',
                    TIMESTAMP '2026-08-11 09:30:00'
                ),
                (
                    4,
                    101,
                    503,
                    1,
                    CAST(4000.00 AS DECIMAL(12, 2)),
                    'paid',
                    TIMESTAMP '2026-08-11 13:00:00'
                )
        """)

    spark.sql("""
            SELECT *
            FROM lake.sales.orders
            ORDER BY order_id
        """).show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()