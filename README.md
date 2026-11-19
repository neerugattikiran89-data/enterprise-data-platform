\# Enterprise Data Platform with Orchestration and Data Quality



\## Overview

This project implements an enterprise-style data engineering platform using PostgreSQL, Apache Airflow, dbt, Docker, and Debezium. It demonstrates data ingestion, database integration, transformation, orchestration, and analytics workflows using an e-commerce dataset.



\## Architecture



CSV / REST API Sources → PostgreSQL Source Database → Data Pipeline \& Orchestration (Airflow) → PostgreSQL Data Warehouse → dbt Transformations → Analytics



CDC integration is configured using Debezium.



\## Technology Stack

\- \*\*Python\*\* – Data ingestion and pipeline development

\- \*\*PostgreSQL\*\* – Source database and data warehouse

\- \*\*Apache Airflow\*\* – Workflow orchestration

\- \*\*dbt\*\* – Data transformation and modeling

\- \*\*Docker \& Docker Compose\*\* – Containerized infrastructure

\- \*\*Debezium\*\* – Change Data Capture integration

\- \*\*CSV \& REST API\*\* – Data ingestion sources



\## Project Structure



```text

├── airflow/

│   └── dags/

├── data/

│   └── brazilian\_dataset/

├── docs/

├── ecommerce\_dbt/

│   ├── models/

│   │   ├── staging/

│   │   └── marts/

│   └── tests/

├── postgres/

│   └── init/

├── docker-compose.yml

├── debezium-postgres-connector.json

└── README.md

```



\## Key Components

\- CSV data ingestion

\- REST API ingestion workflow

\- PostgreSQL source database

\- PostgreSQL data warehouse

\- Airflow DAG orchestration

\- dbt staging and analytical models

\- Debezium CDC configuration

\- Data pipeline documentation and testing



\## Dataset

Brazilian e-commerce dataset containing customer, order, payment, product, seller, review, and geolocation data.



\## Setup



1\. Clone the repository:



```bash

git clone https://github.com/neerugattikiran89-data/enterprise-data-platform.git

cd enterprise-data-platform

```



2\. Start the services:



```bash

docker compose up -d

```



3\. Access the services using the ports and credentials configured in `docker-compose.yml`.



4\. Run and monitor the pipeline workflows using the configured Airflow environment.



5\. Execute dbt models and tests from the `ecommerce\_dbt` project directory.



\## Documentation



Additional documentation is available in the `docs/` directory:

\- Data and API documentation

\- Test results

\- Individual project reflection

\- Presentation outline



\## Project Objective

To develop an end-to-end data engineering workflow demonstrating ingestion, orchestration, data transformation, database integration, and analytics using modern data engineering tools.



