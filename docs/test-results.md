# Test Results

## 1. CSV Ingestion

Successfully loaded nine Olist CSV datasets into the source PostgreSQL database.

| Dataset | Records |
|---|---:|
| Customers | 99,441 |
| Geolocation | 1,000,163 |
| Orders | 99,441 |
| Order items | 112,650 |
| Payments | 103,886 |
| Reviews | 99,224 |
| Products | 32,951 |
| Sellers | 3,095 |
| Category translation | 71 |
| **Total** | **1,560,922** |

## 2. REST API Ingestion
- JSONPlaceholder API ingestion implemented using Airflow.
- 100 initial API records verified.
- An additional record was inserted for CDC testing.

## 3. CDC Testing
- Debezium PostgreSQL connector configured.
- Kafka topic: `ecommerce.raw.api_posts`.
- Insert event successfully observed with operation type `c`.

## 4. dbt Testing
- Customer staging model created successfully.
- Unique and not-null tests were defined.
- Initial dbt build was partially successful.
- Orders staging and downstream models require the warehouse orders table to be available before final validation.

## 5. Final Validation
Final warehouse transfer and dbt build results must be updated after the pipeline finishes. No unverified test results are claimed.
