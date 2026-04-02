# Data Ingestion DAGs

---
### awc_metar_to_s3.py

Airflow DAG that ingests recent KSFO METAR data from the Aviation Weather Center API and stores new observations in S3. A lightweight local state file is used to track the latest processed record, ensuring overlapping API responses do not create duplicate data.
