# Project: Real-Time Streaming Dashboard

A production-style reference for **Kafka → Spark Structured Streaming → Redshift → Power BI** with
large synthetic data, orchestration, monitoring, and load testing.

## Highlights
- **Kafka** topics (`orders`, `clicks`) with JSON schemas
- **Spark Structured Streaming** job: parse → enrich → aggregate → write
- **Redshift** sink (JDBC) with upserts for hourly aggregates
- **Large bootstrap data** (~250k events) for realistic load & backfills
- **Airflow DAG** to manage Spark job + health checks
- **Load/Stress tools** to push events into Kafka at configurable TPS
- **Power BI** notes for DirectQuery/Import & incremental refresh
- **Docs** covering kickoff, design, monitoring, stress plan, retrospective
- **CI** pipeline (flake8 + unit tests on helpers)


