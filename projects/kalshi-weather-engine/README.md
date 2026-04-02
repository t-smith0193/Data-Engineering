# kalshi-weather-engine

---
This project is an attempt at building a real-time trading engine for weather-based markets on Kalshi. It’s built on top of a data pipeline that ingests aviation weather data and Kalshi market prices, then tries to connect forecasts, observed conditions, and market behavior.

The infrastructure runs on an AWS EC2 (Linux) instance with Dockerized Airflow handling scheduled ingestion and transformations. Data is stored in S3 as a central data lake, where both raw and processed datasets (JSON, CSV, parquet) live. Batch workflows are used to understand how weather evolves throughout the day and how that maps to market outcomes.

On top of that, there’s a layer of event-driven AWS Lambda functions that react to new data in real time. These handle things like alerting, Slack updates, anomaly detection, and triggering downstream analysis. There’s also an OpenAI-powered component that takes NWS forecast discussions (AFDs), filters them down to KSFO-relevant signals, and turns them into structured forecasts mapped directly to Kalshi market buckets.

The goal is to combine batch modeling (Airflow + DuckDB + S3) with real-time reactions (Lambda + Slack) to continuously compare live weather and forecasts against current market pricing—and surface useful signals as they happen.
<br><br>

