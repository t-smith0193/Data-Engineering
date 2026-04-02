from datetime import datetime, timedelta
import json
import os
from typing import Any, Dict, List, Optional

import boto3
import requests

from airflow import DAG
from airflow.operators.python import PythonOperator


# AWS + storage configuration.
AWS_REGION = os.getenv("AWS_REGION", "us-east-2")
S3_BUCKET = "weather-kalshi-data"

# Incremental storage path (append-only writes).
S3_PREFIX = "raw/awc/metar/incremental/KSFO"

# Local state file to track last processed METAR.
STATE_FILE = "/opt/airflow/state/awc_metar_state.json"

# API + station configuration.
STATION = "KSFO"
METAR_URL = "https://aviationweather.gov/api/data/metar?ids=KSFO&format=json&hours=2"
USER_AGENT = "weather-kalshi-pipeline/0.1"


def load_state() -> Dict[str, Any]:
    """
    Load last processed state from disk.

    This allows the pipeline to only ingest new METAR observations
    instead of reprocessing the same data repeatedly.
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
    Persist state atomically to avoid corruption.

    Writes to a temp file and replaces the original file.
    """
    tmp_file = f"{STATE_FILE}.tmp"
    with open(tmp_file, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2, sort_keys=True)
    os.replace(tmp_file, STATE_FILE)


def make_record_id(record: Dict[str, Any]) -> str:
    """
    Build a unique identifier for a METAR record.

    Combines:
      - station
      - report time
      - raw observation text

    This helps detect updates even when timestamps match.
    """
    icao_id = record.get("icaoId", "")
    report_time = record.get("reportTime", "")
    raw_ob = record.get("rawOb", "")
    return f"{icao_id}|{report_time}|{raw_ob}"


def parse_iso_z(ts: Optional[str]) -> Optional[datetime]:
    """
    Parse ISO timestamps with optional 'Z' (UTC) suffix.
    """
    if not ts:
        return None
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))


def fetch_metar_to_s3() -> None:
    """
    Fetch recent METAR data and write only new records to S3.

    Workflow:
      1. Pull latest METARs (last ~2 hours).
      2. Compare against stored state.
      3. Select only new or updated records.
      4. Write batch to S3.
      5. Update state.
    """
    response = requests.get(
        METAR_URL,
        headers={"User-Agent": USER_AGENT},
        timeout=30,
    )
    response.raise_for_status()

    data = response.json()

    if not data:
        print("No METAR records returned.")
        return

    # Sort to ensure deterministic processing order.
    data_sorted = sorted(data, key=lambda x: x.get("reportTime", ""))

    state = load_state()
    station_state = state.get(STATION, {})

    latest_report_time_str = station_state.get("latest_report_time")
    latest_record_id = station_state.get("latest_record_id")

    latest_report_time = parse_iso_z(latest_report_time_str)

    new_records: List[Dict[str, Any]] = []

    for record in data_sorted:
        record_report_time_str = record.get("reportTime")
        record_report_time = parse_iso_z(record_report_time_str)
        record_id = make_record_id(record)

        if latest_report_time is None:
            # First run: ingest everything.
            new_records.append(record)
            continue

        if record_report_time is None:
            # Skip malformed records.
            continue

        if record_report_time > latest_report_time:
            # New observation.
            new_records.append(record)
        elif record_report_time == latest_report_time and record_id != latest_record_id:
            # Same timestamp but different content → updated observation.
            new_records.append(record)

    if not new_records:
        print("No new METAR records found. Skipping S3 write.")
        return

    now = datetime.utcnow()

    # Wrap records with metadata for traceability.
    payload = {
        "ingest_ts_utc": now.isoformat() + "Z",
        "source": "awc",
        "product": "metar",
        "station": STATION,
        "request_url": METAR_URL,
        "new_record_count": len(new_records),
        "records": new_records,
    }

    payload_str = json.dumps(payload, sort_keys=True)

    s3 = boto3.client("s3", region_name=AWS_REGION)

    # Append-only write using ingestion timestamp.
    key = (
        f"{S3_PREFIX}/"
        f"{now.strftime('%Y-%m-%dT%H-%M-%SZ')}_ksfo_metar.json"
    )

    s3.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=payload_str.encode("utf-8"),
        ContentType="application/json",
    )

    # Update state using the newest processed record.
    newest_record = max(new_records, key=lambda x: x.get("reportTime", ""))
    newest_record_id = make_record_id(newest_record)

    state[STATION] = {
        "latest_report_time": newest_record.get("reportTime"),
        "latest_record_id": newest_record_id,
    }
    save_state(state)

    print(f"Wrote {len(new_records)} new records to s3://{S3_BUCKET}/{key}")
    print(f"Updated state for {STATION}: {state[STATION]}")


# Standard Airflow defaults.
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="awc_metar_ksfo_to_s3",
    default_args=default_args,
    description="Pull KSFO METAR data from AWC API and write only new raw JSON records to S3",
    start_date=datetime(2026, 3, 23),
    schedule="0-5,10,50,55-59 * * * *",  # Frequent polling to catch near-real-time updates
    catchup=False,
    tags=["weather", "awc", "metar", "s3"],
) as dag:

    # Single-task DAG performing incremental ingestion.
    fetch_and_store = PythonOperator(
        task_id="fetch_metar_to_s3",
        python_callable=fetch_metar_to_s3,
    )