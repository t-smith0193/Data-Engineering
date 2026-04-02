# Lambda Functions

This repository also includes AWS Lambda functions that support alerting, scraping, summarization, and AI-driven market analysis for KSFO weather workflows.  

The Lambdas follow a similar pattern to the Airflow pipelines:
- Event-driven processing from S3, SNS, API Gateway, or scheduled triggers  
- Lightweight transformations and validation before downstream actions  
- Slack notifications for operational visibility  
- Structured outputs written to S3 for traceability and downstream modeling  

---

### afd_recommendation  
AWS Lambda function that reacts to newly ingested KSFO Area Forecast Discussion (AFD) text files, extracts forecast sections relevant to coastal San Francisco weather, and uses an OpenAI model to generate a structured daily max temperature forecast mapped to Kalshi market buckets. The function builds a constrained prompt using filtered forecast text and market metadata, validates and normalizes the model output, writes a forecast snapshot to S3, and posts a summary with bucket-level pricing comparisons to Slack.

---

### ksfo_brief  
AWS Lambda function that processes newly arrived KSFO weather JSON files from S3 and posts human-readable Slack updates for METAR and TAF products. For METAR updates, it also computes the current SF-local-day high and low temperature range and asynchronously triggers downstream market analysis using the latest observation context.

---

### ksfo_slash_trend  
AWS Lambda function that powers a Slack slash command for viewing the latest KSFO MADIS temperature trend. It reads the most recent intraday CSV snapshot from S3, extracts usable temperature rows, and returns a channel-visible summary including the latest reading, current day high, and the last hour of 5-minute observations.

---

### ksfo_spike_alert  
AWS Lambda function that monitors MADIS CSV snapshot uploads in S3 and alerts Slack when the MADIS daily high exceeds the METAR daily high for the same SF-local date. It maintains per-day state in S3 to prevent duplicate alerts and supports repeat alerts when confidence increases through repeated high-temperature occurrences.

---

### nws-afd-ingestor  
AWS Lambda function that scrapes the latest National Weather Service Area Forecast Discussion for the San Francisco forecast office, extracts the clean raw product text, and stores new versions in S3. It uses a lightweight checkpoint file in S3 to make ingestion idempotent and avoid rewriting the same issuance multiple times.

---

### sns-to-slack  
AWS Lambda function that forwards AWS SNS notifications to Slack using an incoming webhook. It acts as a simple bridge between AWS-native alerting and Slack messaging so system or pipeline events can be surfaced directly in chat.
