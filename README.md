# Airbnb Data Engineering Pipeline

End-to-end ELT pipeline processing 5.37 million Airbnb reviews
and 279,712 listings across 10 global cities.

## Tech Stack
- Apache Airflow — orchestration
- DBT — data transformation
- Snowflake — cloud data warehouse
- AWS S3 — raw data storage
- Python — pipeline logic
- Docker — containerisation

## Architecture
S3 → Airflow → Snowflake Bronze → DBT Silver → DBT Gold → Power BI

## Pipeline
7 Airflow tasks run daily:
1. check_s3_files
2. load_listings_to_bronze
3. load_reviews_to_bronze
4. validate_bronze_load
5. run_dbt_transformations (8 models)
6. run_dbt_tests (40 tests)
7. validate_gold_layer

## Data
- 279,712 listings across 10 cities
- 5,373,143 reviews
- 4 Gold fact tables
- 40 DBT data quality tests