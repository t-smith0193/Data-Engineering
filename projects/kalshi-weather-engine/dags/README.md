# Data Ingestion DAGs

This repository contains production-style Airflow pipelines for ingesting, transforming, and modeling weather data for use in Kalshi market analysis.  

The pipelines follow a consistent pattern of:
- Incremental ingestion from external APIs and public datasets  
- Deduplication and state tracking to avoid duplicate records  
- Storage of raw and processed data in S3  
- Transformation into structured datasets for downstream analytics and modeling  

---

### awc_metar_to_s3.py  
Ingests recent KSFO METAR data from the Aviation Weather Center API and stores new observations in S3. A lightweight local state file is used to track the latest processed record, ensuring overlapping API responses do not create duplicate data.

---

### awc_metar_ksfo_to_s3.py  
Incrementally ingests KSFO METAR observations from the Aviation Weather Center API and writes only new records to S3. Uses a local state file and record-level deduplication to handle high-frequency updates and same-timestamp revisions.

---

### awc_taf_ksfo_to_s3.py  
Ingests KSFO TAF (forecast) data from the Aviation Weather Center API and stores only new or updated forecast records in S3. Maintains a local state file to prevent duplicate ingestion and detect updates with identical timestamps.

---

### current_day_market_weather_lookup.py  
Script executed via Airflow that builds a current-day lookup table linking Kalshi weather markets with historical intraday weather models. Uses DuckDB to query S3 data, compute temperature progression features, and export a parquet dataset for downstream modeling or pricing.

---

### kalshi_kxhightsfo_candlesticks_to_s3.py  
Retrieves intraday candlestick data for Kalshi weather markets and stores it in S3. Uses existing market metadata to determine which events to process and refreshes recent, non-finalized dates to ensure data completeness.

---

### kalshi_kxhightsfo_market_metadata_to_s3.py  
Ingests Kalshi market metadata for a specific weather series and stores one JSON document per event date in S3. Implements incremental ingestion with selective refresh of recent non-finalized events.

---

### madis_ksfo_5min_finalize.py  
Consolidates intraday MADIS weather snapshots into a single daily `final.csv` file in S3. Selects the latest snapshot for the day and removes intermediate files to produce a clean, stable daily dataset.

---

### madis_ksfo_5min_ingest.py  
Ingests near-real-time MADIS weather observations, transforms NetCDF data into structured temperature records, and writes incremental CSV snapshots to S3. Merges new data with existing daily snapshots to maintain a continuously updated dataset.

---

### nws_cli_ksfo_to_s3_json.py  
Scrapes National Weather Service CLI (climate summary) reports, parses structured weather metrics, and stores canonical daily reports in S3. Selects the most reliable version of each report based on issuance timing and finalization status.

---

### system_health_heartbeat.py  
Runs hourly to monitor system health (CPU, memory, disk usage) and sends a heartbeat notification via AWS SNS. Flags potential issues when resource usage exceeds defined thresholds.
