# kalshi-weather-engine

---
This project is an attempt at a real-time trading engine for weather-based markets on Kalshi, built on top of a data pipeline that ingests aviation weather data and Kalshi market prices. It starts with batch data to understand how forecasts, observed conditions, and market outcomes relate, then will transition into a streaming system that monitors live updates against current pricing.  
<br><br>
Build Steps Completed:  

\- Set up Dockerized Airflow on AWS EC2  
\- Created S3 bucket structure for raw weather data.  
\- Backfilled recent KSFO METAR history from the Aviation Weather Center.  
\- Created the first Airflow DAG for incremental METAR ingestion.  
\- Solved duplicate-write issues caused by overlapping API request windows.  
\- Added persistent state tracking for latest ingested METAR records.  
\- Confirmed successful writes to S3 from the running DAG.  
