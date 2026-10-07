# ⚡ Real-Time Transactions Pipeline
 
A **real-time** data pipeline for simulated financial transactions, built end to end with **Redpanda (Kafka), Spark Structured Streaming, Delta Lake on S3, Snowflake and dbt**, orchestrated with **Airflow**.
 
> 🚧 **Status:** under active development, with daily commits and documentation. Follow the progress in [`docs/devlog`](docs/devlog) and the full plan in [`ROADMAP.md`](ROADMAP.md).
 
---
 
## 🎯 Goal
 
Build and document a streaming pipeline that deals with real-world problems: **duplicate, late, out-of-order and invalid events, and schema changes**.
 
The focus is not just making data flow, but **demonstrating and proving** how the pipeline behaves when something goes wrong: recovery after failure, reprocessing, idempotency and consistency across layers.
