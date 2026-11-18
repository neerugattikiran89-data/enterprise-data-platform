# Final Presentation – Enterprise Data Platform

## Slide 1: Project Introduction (1 minute)
- Project: Enterprise Data Platform with Orchestration and Data Quality
- Domain: Data Engineering
- Objective: Build an automated e-commerce data ingestion and transformation pipeline.

## Slide 2: Problem Statement (1 minute)
- E-commerce data comes from multiple sources and formats.
- Manual ingestion and processing are difficult to maintain.
- A centralized and automated data pipeline is needed.

## Slide 3: Technology Stack (1 minute)
- Python
- PostgreSQL
- Apache Airflow
- Apache Kafka
- Debezium
- dbt
- Docker Compose

## Slide 4: Architecture (1 minute)
Explain the flow:
CSV/API sources → Airflow → Source PostgreSQL → Warehouse PostgreSQL → dbt transformations.
Debezium captures database changes and publishes events to Kafka.

## Slide 5: Data Ingestion (1 minute)
- Nine Olist CSV datasets.
- 1,560,922 CSV records loaded.
- REST API ingestion using JSONPlaceholder.

## Slide 6: Orchestration and CDC (1 minute)
- Airflow DAGs automate ingestion and transfer workflows.
- Debezium captures changes from PostgreSQL.
- Kafka receives CDC events.

## Slide 7: Warehouse and Transformation (1 minute)
- Separate source and warehouse databases.
- dbt staging models for customers and orders.
- Incremental fact orders model.
- Unique and not-null tests.

## Slide 8: Challenges (1 minute)
- Docker service communication.
- Database connectivity and configuration.
- Large CSV data transfers.
- Integrating Airflow, PostgreSQL, Kafka and dbt.

## Slide 9: Results and Demo (1 minute)
- Show CSV ingestion results.
- Show Airflow DAGs.
- Show Kafka CDC topic.
- Show warehouse tables and dbt models.
- Present final verified test results.

## Slide 10: Learning and Next Steps (1 minute)
- Practical experience with data engineering tools.
- Better understanding of orchestration and CDC.
- Future improvements: Great Expectations, DataHub lineage, monitoring and alerting.
- Thank you.
