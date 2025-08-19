from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

CONN_ID = "APP_POSTGRES"

default_args = {
    "owner": "data-eng",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="orders_daily_summary",
    default_args=default_args,
    start_date=datetime(2025, 8, 13),
    schedule_interval="0 0 * * *",   # diario 00:00 UTC
    catchup=True,                    # permite backfill
    max_active_runs=1,
    tags=["order", "metrics"],
) as dag:

    create_table = PostgresOperator(
        task_id="create_table",
        postgres_conn_id=CONN_ID,
        sql="""
        CREATE TABLE IF NOT EXISTS orders_daily_summary (
            day date NOT NULL,
            customer_id text NOT NULL,
            orders integer NOT NULL,
            total_amount numeric(14,2) NOT NULL,
            PRIMARY KEY (day, customer_id)
        );
        """,
    )

    # cada run procesa [data_interval_start, data_interval_end)
    upsert_interval = PostgresOperator(
        task_id="upsert_interval",
        postgres_conn_id=CONN_ID,
        sql="""
        WITH base AS (
          SELECT
            date_trunc('day', created_at)::date AS day,
            customer_id,
            COUNT(order_id) AS orders,
            COALESCE(SUM(amount),0)::numeric(14,2) AS total_amount
          FROM orders
          WHERE created_at >= '{{ data_interval_start }}'::timestamptz
            AND created_at <  '{{ data_interval_end }}'::timestamptz
          GROUP BY 1,2
        )
        INSERT INTO orders_daily_summary (day, customer_id, orders, total_amount)
        SELECT day, customer_id, orders, total_amount
        FROM base
        ON CONFLICT (day, customer_id)
        DO UPDATE SET
          orders = EXCLUDED.orders,
          total_amount = EXCLUDED.total_amount;
        """,
    )

    create_table >> upsert_interval
