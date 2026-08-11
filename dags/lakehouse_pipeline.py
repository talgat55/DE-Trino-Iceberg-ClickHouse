from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


SPARK_PACKAGES = (
    "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0,"
    "org.apache.hadoop:hadoop-aws:3.4.1,"
    "org.postgresql:postgresql:42.7.4,"
    "com.clickhouse:clickhouse-jdbc:0.9.2"
)


def spark_submit(script: str) -> str:
    return (
        "docker exec lakehouse_spark "
        "/opt/spark/bin/spark-submit "
        "--master 'local[1]' "
        "--driver-memory 384m "
        "--conf spark.executor.memory=384m "
        "--conf spark.driver.maxResultSize=128m "
        "--conf spark.jars.ivy=/opt/spark-ivy "
        "--py-files /opt/spark-apps/spark_common.py "
        f"--packages {SPARK_PACKAGES} "
        f"{script}"
    )


default_args = {
    "owner": "t",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="lakehouse_trino_clickhouse_pipeline",
    start_date=datetime(2026,8,1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["spark", "iceberg", "trino", "clickhouse"],
) as dag:

    build_iceberg = BashOperator(
        task_id="build_iceberg",
        bash_command=spark_submit("/opt/spark-apps/create_orders_iceberg.py"),
    )

    check_trino = BashOperator(
        task_id="check_trino",
        bash_command=(
            "docker start lakehouse_trino && "
            "for i in $(seq 1 40); do "
            "docker exec lakehouse_trino trino --execute 'SELECT 1' >/dev/null 2>&1 && break; "
            "sleep 5; "
            "done && "
            "docker exec lakehouse_trino trino --execute "
            "\"SELECT COUNT(*) FROM iceberg.sales.orders\""
        ),
    )

    load_clickhouse = BashOperator(
        task_id="load_clickhouse",
        bash_command=spark_submit(
            "/opt/spark-apps/load_customer_sales_to_clickhouse.py"
        ),
    )

    check_clickhouse = BashOperator(
        task_id="check_clickhouse",
        bash_command="""
        docker exec lakehouse_clickhouse \
        clickhouse-client \
        --user analytics \
        --password analytics_pass \
        --query "
        SELECT count(*)
        FROM analytics.customer_sales
        "
        """,
    )

    build_iceberg >> check_trino >> load_clickhouse >> check_clickhouse
