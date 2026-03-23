# Runtime State Management

---
To avoid duplicate data, the ingestion DAG keeps a lightweight local state file that tracks the latest METAR observation. Since the API returns overlapping time windows, this ensures only new records are stored each run.

Example structure:

```json
{
  "KSFO": {
    "latest_report_time": "2026-03-23T20:56:00Z",
    "latest_record_id": "KSFO|2026-03-23T20:56:00Z|..."
  }
}
