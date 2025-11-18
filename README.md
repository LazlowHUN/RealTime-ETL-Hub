# 🚀 Real-Time Data Pipeline with Kafka, Airflow, dbt & Snowflake
An end-to-end pipeline simulating an e-commerce shop: from generating and streaming user events, orchestrating transformations via Airflow + Python, loading into Snowflake, to interactive visualizations in Power BI. Highlights data engineering best practices.

## End-to-End Streaming → Warehouse → Transformation Pipeline
This project demonstrates a modern, production-grade data engineering architecture that ingests real-time events via Kafka, loads raw data into Snowflake, transforms it using dbt, and orchestrates the entire flow with Apache Airflow.

## 🧱 Architecture Overview

## 📦 Key Components
### 1. Kafka Event Streaming
A Python Kafka producer generates synthetic user interaction events (e.g., view, add_to_cart, purchase) and publishes them to a Kafka topic.

Example event:

```
{
  "event_id": "79b702a3-950f-47a4-a0b4",
  "event_type": "view",
  "user_id": "user_24",
  "product_id": "mug",
  "price": 118.91,
  "currency": "EUR",
  "quantity": 5,
  "session_id": "6a8a26e8-c9a0-44f6",
  "ts": "2025-11-12T22:27:16.88Z",
  "user_agent": "Mozilla/5.0 ..."
}
```

Exposed via (Kafka UI):
👉 http://localhost:8080

<img width="1895" height="353" alt="image" src="https://github.com/user-attachments/assets/b7a87661-79a6-4352-8113-b1a12ebcc86b" />

A Kafka consumer persists events into Snowflake RAW.

### 2. Snowflake RAW Layer

<img width="305" height="341" alt="image" src="https://github.com/user-attachments/assets/62765463-a21f-4d30-aae9-905cfed6e0b9" />

Stores unmodified ingestion-ready data:

| Column  | Type | Description |
| ------------- | ------------- | ------------- |
| EVENT_ID  | VARCHAR  | Unique event ID |
| RAW_DATA  | VARIANT  | Full JSON payload |
| RECEIVED_AT  | TIMESTAMP_NTZ  | Ingestion timestamp |

<img width="1270" height="117" alt="image" src="https://github.com/user-attachments/assets/af33ed91-5353-4653-b393-f032cccfb923" />

This layer acts as the immutable "source of truth".

### 3. Snowflake - dbt Staging Layer
Model: stg_events.sql

Purpose:

* Flatten the JSON from RAW layer
* Cast and clean fields
* Validate event structure
* Add quality flags (is_valid, error_reason)

Example logic includes:

```
select
  raw_data:event_id::string as event_id,
  raw_data:event_type::string as event_type,
  raw_data:user_id::string as user_id,
  ...
  received_at as ts,
  raw_data as raw_payload,
  true as is_valid
from raw.events_json
```

dbt Tests Included:

* unique (event_id)
* not_null (key fields)
* accepted_values (event_type)
* data integrity checks

<img width="1461" height="121" alt="image" src="https://github.com/user-attachments/assets/c2e90b22-fe0a-4504-88b0-dc37f213799f" />

### 4. Snowflake - dbt MARTS Layer
Business-ready analytics tables.

Model: fct_events.sql

Aggregates metrics per event type and user:

```
select
  date(ts) as event_date,
  event_type,
  user_id,
  count(*) as event_count,
  sum(quantity) as total_quantity,
  sum(price * quantity) as total_revenue
from {{ ref('stg_events') }}
where is_valid = true
group by 1,2,3
```

Use cases:

* Funnel metrics
* Revenue analysis
* User behaviour analytics
* Product performance

<img width="877" height="122" alt="image" src="https://github.com/user-attachments/assets/105b520c-7537-4943-8799-0080f9b9b55c" />

### 5. Airflow Orchestration
DAG: etl_dbt_pipeline

<img width="1903" height="772" alt="image" src="https://github.com/user-attachments/assets/61503600-7d16-4db6-bd72-297b6a4a324e" />

Tasks:

1. Run dbt staging
Builds staging models.

2. Run dbt tests (staging)
Ensures data quality before loading marts.

4. Run dbt marts
Builds fact models.

5. Run dbt tests (marts)
Validates aggregated data.

Exposed via:
👉 http://localhost:8081

### 6. dbt Documentation UI

<img width="1902" height="930" alt="image" src="https://github.com/user-attachments/assets/3a89800c-d937-41e1-bd2b-950ae18d2a51" />

Features:

* Interactive lineage graph
* Model & column-level documentation
* Built-in test visibility
* SQL preview
* Tables grouped by schema (RAW, STAGING, MARTS)

Exposed via:
👉 http://localhost:8082
