
import requests
import psycopg2

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


API_URL = "https://jsonplaceholder.typicode.com/posts"


def ingest_api_data():

    # Fetch data from REST API
    response = requests.get(API_URL, timeout=30)
    response.raise_for_status()

    posts = response.json()

    # Connect to PostgreSQL
    conn = psycopg2.connect(
        host="source-postgres",
        database="ecommerce_source",
        user="source_user",
        password="source_password",
        port=5432
    )

    try:
        with conn.cursor() as cursor:

            cursor.execute("""
                CREATE SCHEMA IF NOT EXISTS raw
            """)

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS raw.api_posts (
                    id INTEGER PRIMARY KEY,
                    user_id INTEGER,
                    title TEXT,
                    body TEXT,
                    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            for post in posts:

                cursor.execute("""
                    INSERT INTO raw.api_posts
                    (id, user_id, title, body)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (id) DO UPDATE SET
                        user_id = EXCLUDED.user_id,
                        title = EXCLUDED.title,
                        body = EXCLUDED.body,
                        ingested_at = CURRENT_TIMESTAMP
                """, (
                    post["id"],
                    post["userId"],
                    post["title"],
                    post["body"]
                ))

        conn.commit()

        print(f"Successfully ingested {len(posts)} API records")

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
    dag_id="rest_api_ingestion",
    default_args=default_args,
    description="Ingest REST API data into PostgreSQL raw schema",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["api", "postgres", "ingestion"],
) as dag:

    api_ingestion_task = PythonOperator(
        task_id="fetch_and_load_api_data",
        python_callable=ingest_api_data,
    )
