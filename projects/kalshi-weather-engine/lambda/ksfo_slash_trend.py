import csv
import io
import json
import os
import urllib.parse
from datetime import datetime
from zoneinfo import ZoneInfo

import boto3

# Reuse S3 client across Lambda invocations.
s3 = boto3.client("s3")

# S3 location for MADIS snapshot data.
BUCKET = os.environ.get("BUCKET", "weather-kalshi-data")
BASE_PREFIX = os.environ.get("BASE_PREFIX", "raw/madis/KSFO/5-min")

# Timezones used for display formatting.
PT_TZ = ZoneInfo("America/Los_Angeles")
ET_TZ = ZoneInfo("America/New_York")


def list_day_csv_keys(bucket: str, prefix: str) -> list[str]:
    """
    List all CSV snapshot files for a given day (excluding final.csv).

    Returns sorted keys so the latest snapshot is always last.
    """
    paginator = s3.get_paginator("list_objects_v2")
    keys = []

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".csv") and not key.endswith("/final.csv"):
                keys.append(key)

    return sorted(keys)


def read_text_from_s3(bucket: str, key: str) -> str:
    """
    Read a text file from S3 and return its contents as a string.
    """
    resp = s3.get_object(Bucket=bucket, Key=key)
    return resp["Body"].read().decode("utf-8")


def parse_csv_rows(csv_text: str) -> list[dict]:
    """
    Parse CSV text into structured rows.

    Cleans data by:
      - Skipping missing/invalid values
      - Converting temperature to integer
    """
    reader = csv.DictReader(io.StringIO(csv_text))
    rows = []

    for row in reader:
        valid = (row.get("valid") or "").strip()
        temp_f_raw = (row.get("temp_f") or "").strip()

        if not valid or temp_f_raw in ("", "null", "None"):
            continue

        try:
            temp_f = int(float(temp_f_raw))
        except Exception:
            continue

        rows.append({
            "valid": valid,
            "temp_f": temp_f,
        })

    return rows


def parse_pt_datetime(valid_str: str) -> datetime:
    """
    Convert "valid" string into a timezone-aware Pacific Time datetime.
    """
    return datetime.strptime(valid_str, "%m/%d/%Y %H:%M").replace(tzinfo=PT_TZ)


def format_pt(dt: datetime) -> str:
    """Format datetime for Pacific Time display."""
    return dt.strftime("%-I:%M %p PT")


def format_et(dt: datetime) -> str:
    """Format datetime for Eastern Time display."""
    return dt.astimezone(ET_TZ).strftime("%-I:%M %p ET")


def build_trend_message(bucket: str, base_prefix: str) -> str:
    """
    Build a Slack-friendly summary of today's temperature trend.

    Includes:
      - Latest observation (PT + ET)
      - Current day's high temperature
      - Last hour of 5-minute observations
    """
    now_pt = datetime.now(PT_TZ)
    date_str = now_pt.strftime("%Y-%m-%d")
    prefix = f"{base_prefix}/{date_str}/"

    keys = list_day_csv_keys(bucket, prefix)
    if not keys:
        return f"KSFO MADIS\nNo CSV files found yet for {date_str}."

    # Use latest snapshot for current state.
    latest_key = keys[-1]
    csv_text = read_text_from_s3(bucket, latest_key)
    rows = parse_csv_rows(csv_text)

    if not rows:
        return f"KSFO MADIS\nNo usable rows found in `{latest_key}`."

    # Ensure rows are ordered chronologically.
    rows.sort(key=lambda r: parse_pt_datetime(r["valid"]))

    latest_row = rows[-1]
    latest_dt_pt = parse_pt_datetime(latest_row["valid"])
    latest_temp = latest_row["temp_f"]

    # Compute daily high.
    day_high = max(r["temp_f"] for r in rows)

    # Extract last hour (12 × 5-minute intervals).
    last_12_rows = rows[-12:]
    last_12_temps = ", ".join(str(r["temp_f"]) for r in last_12_rows)

    return (
        "KSFO MADIS\n"
        f"Latest: {format_pt(latest_dt_pt)} / {format_et(latest_dt_pt)}\n"
        f"Today high: {day_high}°F\n"
        f"Last Hour (5M Intervals): {last_12_temps}"
    )


def lambda_handler(event, context):
    """
    AWS Lambda handler for Slack slash command (/trend).

    Workflow:
      1. Parse incoming Slack request (form-encoded body)
      2. Validate command
      3. Build temperature trend message from S3 data
      4. Return formatted Slack response
    """
    # Slack sends payload as x-www-form-urlencoded.
    body = event.get("body", "") or ""

    # Decode base64 if API Gateway encoding is enabled.
    if event.get("isBase64Encoded"):
        import base64
        body = base64.b64decode(body).decode("utf-8")

    params = urllib.parse.parse_qs(body)
    command = (params.get("command", [""])[0] or "").strip()

    # Only support /trend command.
    if command and command != "/trend":
        message = "Unsupported command."
    else:
        message = build_trend_message(BUCKET, BASE_PREFIX)

    # Return Slack-compatible JSON response.
    return {
        "statusCode": 200,
        "headers": {
            "Content-Type": "application/json"
        },
        "body": json.dumps({
            "response_type": "in_channel",  # Visible to entire channel
            "text": message
        })
    }