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

A Kafka consumer persists events into Snowflake RAW.

### 1. Snowflake RAW Layer
Stores unmodified ingestion-ready data:

| Column  | Type | Description |
| ------------- | ------------- | ------------- |
| EVENT_ID  | VARCHAR  | Unique event ID |
| RAW_DATA  | VARIANT  | Full JSON payload |
| RECEIVED_AT  | TIMESTAMP_NTZ  | Ingestion timestamp |