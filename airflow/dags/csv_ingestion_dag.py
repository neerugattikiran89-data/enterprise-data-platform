
import csv
import os
from datetime import datetime, timedelta

import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values

from airflow import DAG
from airflow.operators.python import PythonOperator


DATA_FOLDER = "/opt/airflow/data/brazilian_dataset"

CSV_FILES = [
    "olist_customers_dataset.csv",
    "olist_geolocation_dataset.csv",
    "olist_orders_dataset.csv",
    "olist_order_items_dataset.csv",
    "olist_order_payments_dataset.csv",
    "olist_order_reviews_dataset.csv",
    "olist_products_dataset.csv",
    "olist_sellers_dataset.csv",
    "product_category_name_translation.csv",
]


def load_csv_to_postgres(filename):

    file_path = os.path.join(DATA_FOLDER, filename)

    if not os.path.isfile(file_path):
        raise FileNotFoundError(f"CSV file not found: {file_path}")

    table_name = filename.replace(".csv", "")

    conn = psycopg2.connect(
        host="source-postgres",
        database="ecommerce_source",
        user="source_user",
        password="source_password",
        port=5432
    )

    try:
        with conn.cursor() as cursor:

            cursor.execute(
                "CREATE SCHEMA IF NOT EXISTS raw"
            )

            with open(
                file_path,
                mode="r",
                encoding="utf-8-sig",
                newline=""
            ) as file:

                reader = csv.reader(file)

                headers = next(reader, None)

                if not headers:
                    raise ValueError(f"CSV has no header: {filename}")

                if len(headers) != len(set(headers)):
                    raise ValueError(f"Duplicate column names: {filename}")

                columns = sql.SQL(", ").join(
                    sql.SQL("{} TEXT").format(sql.Identifier(col))
                    for col in headers
                )

                cursor.execute(
                    sql.SQL("DROP TABLE IF EXISTS raw.{}").format(
                        sql.Identifier(table_name)
                    )
                )

                cursor.execute(
                    sql.SQL("CREATE TABLE raw.{} ({})").format(
                        sql.Identifier(table_name),
                        columns
                    )
                )

                insert_query = sql.SQL(
                    "INSERT INTO raw.{} ({}) VALUES %s"
                ).format(
                    sql.Identifier(table_name),
                    sql.SQL(", ").join(
                        sql.Identifier(col) for col in headers
                    )
                )

                batch = []
                total_rows = 0

                for row in reader:

                    if len(row) != len(headers):
                        raise ValueError(
                            f"Invalid row length in {filename}"
                        )

                    batch.append(tuple(row))

                    if len(batch) >= 1000:

                        execute_values(
                            cursor,
                            insert_query.as_string(conn),
                            batch,
                            page_size=1000
                        )

                        total_rows += len(batch)
                        batch.clear()

                if batch:

                    execute_values(
                        cursor,
                        insert_query.as_string(conn),
                        batch,
                        page_size=1000
                    )

                    total_rows += len(batch)

        conn.commit()

        print(
            f"Successfully loaded {filename}: "
            f"{total_rows} rows into raw.{table_name}"
        )

    except Exception:
        conn.rollback()
        raise

    finally:
        conn.close()


default_args = {
    "owner": "data_engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


with DAG(
    dag_id="olist_csv_ingestion",
    default_args=default_args,
    description="Ingest Olist CSV files into PostgreSQL raw schema",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_tasks=2,
    tags=["csv", "postgres", "olist"],
) as dag:

    previous_task = None

    for filename in CSV_FILES:

        task = PythonOperator(
            task_id=f"load_{filename.replace('.csv', '')}",
            python_callable=load_csv_to_postgres,
            op_kwargs={"filename": filename},
        )

        if previous_task:
            previous_task >> task

        previous_task = task
