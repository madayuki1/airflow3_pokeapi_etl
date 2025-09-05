# ETL with Airflow, Google Cloud Storage, Bigquery on PokeAPI data
ETL from pokeapi api data source to Data Warehouse
## Architecture Overview
<img width="621" height="421" alt="airflow3_pokeapi drawio" src="https://github.com/user-attachments/assets/0f9c15dd-99ad-4eda-a90a-166c110cd782" />

## Data Sources
1. PokeAPI (JSON)

## Tech Stacks
1. Python 3.12.3
2. Airflow 3.0.2
3. Google Cloud Storage
4. BigQuery

## Airflow Task

<img width="1580" height="786" alt="airflow_grid" src="https://github.com/user-attachments/assets/bcafc02c-db0c-4eea-a5cb-178bca89e212" />
<img width="1420" height="801" alt="airfow_graph" src="https://github.com/user-attachments/assets/68327de3-0228-4e7c-a8a9-12d81783d9ca" />

## Uploaded Files on Google Cloud Storage

API data pulled from pokeapi
<img width="1919" height="656" alt="gcs" src="https://github.com/user-attachments/assets/a5f3fa00-1769-43a6-88e8-d38d6aa0d569" />
schema for pulled data to be inferred before pushing into bigquery
<img width="1919" height="432" alt="gcs_schema" src="https://github.com/user-attachments/assets/7a3ad078-7abc-4fb6-b84d-51d0b76a0d2c" />

## Created Bigquery Tables

<img width="1222" height="691" alt="bq" src="https://github.com/user-attachments/assets/a53d674b-3a93-4fb9-a1ce-35d7651fa83f" />
example query on move table
<img width="1602" height="744" alt="bq-query" src="https://github.com/user-attachments/assets/e2462d58-002b-4cef-8d26-ffc3219ed874" />
