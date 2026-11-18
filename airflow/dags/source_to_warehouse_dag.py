from datetime import datetime
import logging

import psycopg2
from psycopg2 import sql

from airflow import DAG
from airflow.operators.python import PythonOperator


SOURCE_CONFIG = {
    "host": "source-postgres",
    "port": 5432,
    "database": "ecommerce_source",
    "user": "source_user",
    "password": "source_password",
}

WAREHOUSE_CONFIG = {
    "host": "warehouse-postgres",
    "port": 5432,
    "database": "ecommerce_warehouse",
    "user": "warehouse_user",
    "password": "warehouse_password",
}

TABLES = [
    "api_posts",
    "olist_customers_dataset",
    "olist_geolocation_dataset",
    "olist_order_items_dataset",
    "olist_order_payments_dataset",
    "olist_order_reviews_dataset",
    "olist_orders_dataset",
    "olist_products_dataset",
    "olist_sellers_dataset",
    "product_category_name_translation",
]

BATCH_SIZE = 5000


def transfer_table(table_name):
    source_conn = None
    warehouse_conn = None

    try:
        source_conn = psycopg2.connect(**SOURCE_CONFIG)
        warehouse_conn = psycopg2.connect(**WAREHOUSE_CONFIG)

        with source_conn.cursor() as schema_cursor:
            schema_cursor.execute(
                sql.SQL("SELECT * FROM raw.{} LIMIT 0").format(
                    sql.Identifier(table_name)
                )
            )
            columns = [desc[0] for desc in schema_cursor.description]

        with source_conn.cursor(name=f"source_{table_name}") as source_cursor:
            source_cursor.execute(
                sql.SQL("SELECT * FROM raw.{}").format(
                    sql.Identifier(table_name)
                )
            )

            with warehouse_conn.cursor() as warehouse_cursor:
                warehouse_cursor.execute(
                    "CREATE SCHEMA IF NOT EXISTS raw"
                )

                column_definitions = sql.SQL(", ").join(
                    sql.SQL("{} TEXT").format(sql.Identifier(column))
                    for column in columns
                )

                warehouse_cursor.execute(
                    sql.SQL(
                        "CREATE TABLE IF NOT EXISTS raw.{} ({})"
                    ).format(
                        sql.Identifier(table_name),
                        column_definitions,
                    )
                )

                warehouse_cursor.execute(
                    sql.SQL("SELECT COUNT(*) FROM raw.{}").format(
                        sql.Identifier(table_name)
                    )
                )

                existing_count = warehouse_cursor.fetchone()[0]

                if existing_count > 0:
                    raise RuntimeError(
                        f"Destination raw.{table_name} already contains "
                        f"{existing_count} rows. Skipping to prevent duplicates."
                    )

                insert_query = sql.SQL(
                    "INSERT INTO raw.{} ({}) VALUES ({})"
                ).format(
                    sql.Identifier(table_name),
                    sql.SQL(", ").join(
                        sql.Identifier(column) for column in columns
                    ),
                    sql.SQL(", ").join(
                        sql.Placeholder() for _ in columns
                    ),
                )

                total_rows = 0

                while True:
                    rows = source_cursor.fetchmany(BATCH_SIZE)

                    if not rows:
                        break

                    warehouse_cursor.executemany(
                        insert_query.as_string(warehouse_conn),
                        rows,
                    )

                    total_rows += len(rows)

                warehouse_conn.commit()

                logging.info(
                    "Transferred %s rows from raw.%s",
                    total_rows,
                    table_name,
                )

    except Exception:
        if warehouse_conn:
            warehouse_conn.rollback()
        raise

    finally:
        if source_conn:
            source_conn.close()

        if warehouse_conn:
            warehouse_conn.close()


def transfer_all_tables():
    for table_name in TABLES:
        logging.info("Starting transfer for %s", table_name)
        transfer_table(table_name)

    logging.info("All 10 raw tables transferred successfully.")


with DAG(
    dag_id="source_to_warehouse",
    description="Copy raw PostgreSQL tables from source to warehouse",
    start_date=datetime(2026, 11, 1),
    schedule=None,
    catchup=False,
    tags=["ingestion", "postgresql", "warehouse"],
) as dag:

    transfer_task = PythonOperator(
        task_id="transfer_raw_tables",
        python_callable=transfer_all_tables,
    )

