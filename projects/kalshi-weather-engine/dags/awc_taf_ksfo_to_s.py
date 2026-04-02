from datetime import datetime, timedelta
import json
import os
from typing import Any, Dict, List, Optional

import boto3
import requests

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator


# AWS + storage configuration.
AWS_REGION = os.getenv("AWS_REGION", "us-east-2")
S3_BUCKET = "weather-kalshi-data"
S3_PREFIX = "raw/awc/taf/KSFO"

# Local state file used to track the most recent processed record.
STATE_FILE = "/opt/airflow/state/awc_taf_state.json"

# API + station configuration.
STATION = "KSFO"
TAF_URL = "https://aviationweather.gov/api/data/taf?ids=KSFO&format=json"
USER_AGENT = "weather-kalshi-pipeline/0.1"


def load_state() -> Dict[str, Any]:
    """
    Load the last processed state from disk.

    This allows the DAG to only ingest *new* TAF records instead of
    rewriting the full dataset on every run.
    """
    if not os.path.exists(STATE_FILE):
        return {}

    with open(STATE_FILE, "r", encoding="utf-8") as f:
        content = f.read().strip()
        if not content:
            return {}
        return json.loads(content)


def save_state(state: Dict[str, Any]) -> None:
    """
    Persist state atomically to avoid partial writes.

    Writes to a temp file first, then replaces the original file.
    """
    tmp_file = f"{STATE_FILE}.tmp"
    with open(tmp_file, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2, sort_keys=True)
    os.replace(tmp_file, STATE_FILE)


def make_record_id(record: Dict[str, Any]) -> str:
    """
    Create a unique identifier for a TAF record.

    Combines:
      - station
      - issue time
      - raw TAF text

    This helps detect updates even when timestamps are identical.
    """
    icao_id = record.get("icaoId", "")
    issue_time = record.get("issueTime", "")
    raw_taf = record.get("rawTAF", "")
    return f"{icao_id}|{issue_time}|{raw_taf}"


def parse_iso_z(ts: Optional[str]) -> Optional[datetime]:
    """
    Parse ISO timestamps that may include a trailing 'Z' (UTC).
    """
    if not ts:
        return None
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))


def fetch_taf_to_s3() -> None:
    """
    Fetch latest TAF data from AWC and write only *new* records to S3.

    Core logic:
      1. Pull current API snapshot.
      2. Compare against last processed state.
      3. Select only new/updated records.
      4. Write batch to S3.
      5. Update local state.
    """
    response = requests.get(
        TAF_URL,
        headers={"User-Agent": USER_AGENT},
        timeout=30,
    )
    response.raise_for_status()

    data = response.json()

    if not data:
        print("No TAF records returned.")
        return

    # Sort by issue time so comparisons are deterministic.
    data_sorted = sorted(data, key=lambda x: x.get("issueTime", ""))

    state = load_state()
    station_state = state.get(STATION, {})

    latest_issue_time_str = station_state.get("latest_issue_time")
    latest_record_id = station_state.get("latest_record_id")

    latest_issue_time = parse_iso_z(latest_issue_time_str)

    new_records: List[Dict[str, Any]] = []

    for record in data_sorted:
        record_issue_time_str = record.get("issueTime")
        record_issue_time = parse_iso_z(record_issue_time_str)
        record_id = make_record_id(record)

        if latest_issue_time is None:
            # First run: ingest everything.
            new_records.append(record)
            continue

        if record_issue_time is None:
            # Skip malformed records.
            continue

        if record_issue_time > latest_issue_time:
            # Strictly newer record.
            new_records.append(record)
        elif record_issue_time == latest_issue_time and record_id != latest_record_id:
            # Same timestamp but different content → treat as update.
            new_records.append(record)

    if not new_records:
        print("No new TAF records found. Skipping S3 write.")
        return

    now = datetime.utcnow()

    # Wrap records with metadata for traceability.
    payload = {
        "ingest_ts_utc": now.isoformat() + "Z",
        "source": "awc",
        "product": "taf",
        "station": STATION,
        "request_url": TAF_URL,
        "new_record_count": len(new_records),
        "records": new_records,
    }

    payload_str = json.dumps(payload, sort_keys=True)

    s3 = boto3.client("s3", region_name=AWS_REGION)

    # Partition by ingestion timestamp (append-only pattern).
    key = (
        f"{S3_PREFIX}/"
        f"{now.strftime('%Y-%m-%dT%H-%M-%SZ')}_ksfo_taf.json"
    )

    s3.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=payload_str.encode("utf-8"),
        ContentType="application/json",
    )

    # Update state with newest processed record.
    newest_record = max(new_records, key=lambda x: x.get("issueTime", ""))
    newest_record_id = make_record_id(newest_record)

    state[STATION] = {
        "latest_issue_time": newest_record.get("issueTime"),
        "latest_record_id": newest_record_id,
    }
    save_state(state)

    print(f"Wrote {len(new_records)} new TAF records to s3://{S3_BUCKET}/{key}")
    print(f"Updated state for {STATION}: {state[STATION]}")


# Standard Airflow defaults.
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="awc_taf_ksfo_to_s3",
    default_args=default_args,
    description="Pull KSFO TAF data from AWC API and write only new raw JSON records to S3",
    start_date=datetime(2026, 3, 23),
    schedule="*/15 * * * *",  # Run every 15 minutes to capture updates quickly
    catchup=False,
    tags=["weather", "awc", "taf", "s3"],
) as dag:

    # Single-task DAG that performs incremental ingestion.
    fetch_and_store = PythonOperator(
        task_id="fetch_taf_to_s3",
        python_callable=fetch_taf_to_s3,
    )