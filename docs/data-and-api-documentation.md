# Data and API Documentation

## 1. Dataset
**Name:** Brazilian E-Commerce Public Dataset by Olist

**Format:** CSV

**Storage:** Source PostgreSQL database, schema `raw`.

### Dataset files
- Customers
- Geolocation
- Orders
- Order items
- Payments
- Reviews
- Products
- Sellers
- Product category translation

### Ingestion
Apache Airflow loads CSV records into PostgreSQL in batches. The source-to-warehouse workflow transfers raw tables to a separate PostgreSQL warehouse.

## 2. REST API
**API:** JSONPlaceholder  
**Endpoint:** https://jsonplaceholder.typicode.com/posts  
**Method:** GET  
**Response format:** JSON

Airflow retrieves API records and stores them in the `raw.api_posts` table.

## 3. CDC
Debezium captures PostgreSQL changes for `raw.api_posts` and publishes events to Kafka.

**Kafka topic:** `ecommerce.raw.api_posts`

## 4. Database Overview

| Database | Purpose |
|---|---|
| ecommerce_source | Raw data ingestion |
| ecommerce_warehouse | Data warehouse and dbt transformations |

## 5. Transformation
dbt models:
- `stg_customers`: Customer staging model.
- `stg_orders`: Order staging model.
- `fct_orders`: Incremental orders fact model.

Schema tests include unique and not-null checks on key identifiers.
