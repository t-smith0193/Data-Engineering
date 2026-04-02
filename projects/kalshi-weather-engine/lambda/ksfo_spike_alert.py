# Source: :contentReference[oaicite:0]{index=0}

import csv
import io
import json
import os
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import boto3

# Reuse S3 client across Lambda invocations.
s3 = boto3.client("s3")

# Slack webhook for alert delivery.
SLACK_WEBHOOK_URL = os.environ["SLACK_WEBHOOK_URL"]

# Prefix for storing per-day alert state (prevents duplicate alerts).
MADIS_STATE_PREFIX = os.environ.get(
    "MADIS_STATE_PREFIX",
    "raw/madis/KSFO/5-min/state/",
)

# Prefix where METAR JSON data is stored.
METAR_PREFIX = os.environ["METAR_PREFIX"]

# Timezones for formatting and comparisons.
SF_TZ = ZoneInfo("America/Los_Angeles")
ET_TZ = ZoneInfo("America/New_York")


def post_to_slack(message: str) -> None:
    """
    Send a formatted message to Slack via webhook.

    Raises an error if Slack returns a non-success status.
    """
    payload = json.dumps({"text": message}).encode("utf-8")
    req = urllib.request.Request(
        SLACK_WEBHOOK_URL,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req) as resp:
        if resp.status >= 300:
            raise RuntimeError(f"Slack webhook failed with status {resp.status}")


def c_to_f(c):
    """
    Convert Celsius → Fahrenheit using round-half-up logic.

    Matches NWS-style rounding behavior for whole-degree outputs.
    """
    if c in (None, "", "N/A"):
        return "N/A"
    try:
        temp_f_exact = (float(c) * 9 / 5) + 32
        return int(temp_f_exact + 0.5)
    except Exception:
        return "N/A"


def epoch_to_sf_date(epoch_value):
    """
    Convert epoch timestamp to SF-local calendar date.
    """
    if epoch_value in (None, ""):
        return None
    dt = datetime.fromtimestamp(epoch_value, tz=timezone.utc)
    return dt.astimezone(SF_TZ).date()


def fmt_sf(value: str) -> str:
    """Format timestamp string into readable Pacific Time."""
    if not value:
        return "N/A"
    try:
        dt = datetime.strptime(value, "%m/%d/%Y %H:%M").replace(tzinfo=SF_TZ)
        return dt.strftime("%Y-%m-%d %I:%M %p PT")
    except Exception:
        return value


def fmt_et(value: str) -> str:
    """Format timestamp string into readable Eastern Time."""
    if not value:
        return "N/A"
    try:
        dt = datetime.strptime(value, "%m/%d/%Y %H:%M").replace(tzinfo=SF_TZ)
        return dt.astimezone(ET_TZ).strftime("%Y-%m-%d %I:%M %p ET")
    except Exception:
        return value


def read_text_from_s3(bucket: str, key: str) -> str:
    """Read text file from S3."""
    resp = s3.get_object(Bucket=bucket, Key=key)
    return resp["Body"].read().decode("utf-8")


def read_json_from_s3(bucket: str, key: str) -> dict:
    """Read JSON object from S3."""
    resp = s3.get_object(Bucket=bucket, Key=key)
    return json.loads(resp["Body"].read().decode("utf-8"))


def write_json_to_s3(bucket: str, key: str, payload: dict) -> None:
    """Write JSON object to S3 (used for alert state)."""
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(payload, indent=2, sort_keys=True).encode("utf-8"),
        ContentType="application/json",
    )


def list_metar_keys_newest_first(bucket: str, prefix: str):
    """
    List METAR JSON files in reverse chronological order.

    Allows efficient scanning from newest → oldest.
    """
    paginator = s3.get_paginator("list_objects_v2")
    keys = []

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".json"):
                keys.append(key)

    keys.sort(reverse=True)
    return keys


def get_sf_day_metar_high(bucket: str, prefix: str, target_sf_date):
    """
    Compute the METAR daily high temperature for a given SF-local date.

    Stops early once older dates are reached (optimization).
    """
    high_c = None
    matched_count = 0

    keys = list_metar_keys_newest_first(bucket, prefix)

    for key in keys:
        try:
            data = read_json_from_s3(bucket, key)

            # Only process METAR records.
            if str(data.get("product", "")).lower() != "metar":
                continue

            records = data.get("records", [])
            if not records:
                continue

            record = records[0]
            obs_time = record.get("obsTime")
            temp_c = record.get("temp")

            if obs_time in (None, "") or temp_c in (None, "", "N/A"):
                continue

            obs_sf_date = epoch_to_sf_date(obs_time)

            if obs_sf_date == target_sf_date:
                temp_c = float(temp_c)
                matched_count += 1
                if high_c is None or temp_c > high_c:
                    high_c = temp_c

            # Stop scanning once we pass the target day.
            elif matched_count > 0 and obs_sf_date < target_sf_date:
                break

        except Exception as e:
            print(f"Skipping METAR key {key}: {e}")

    return {
        "high_c": high_c if high_c is not None else None,
        "high_f": c_to_f(high_c) if high_c is not None else "N/A",
        "matched_count": matched_count,
        "date_sf": str(target_sf_date) if target_sf_date is not None else "N/A",
    }


def analyze_madis_csv(csv_text: str):
    """
    Analyze MADIS CSV snapshot to determine:
      - Daily high temperature
      - All times that high occurred
      - Latest occurrence
    """
    reader = csv.DictReader(io.StringIO(csv_text))

    max_temp_f = None
    occurrence_times = []
    total_rows = 0

    for row in reader:
        total_rows += 1
        valid = (row.get("valid") or "").strip()
        temp_f_raw = (row.get("temp_f") or "").strip()

        if not valid:
            continue

        temp_f = None
        if temp_f_raw not in ("", "null", "None"):
            try:
                temp_f = int(float(temp_f_raw))
            except Exception:
                temp_f = None

        if temp_f is None:
            continue

        # Track max temp and all occurrences.
        if max_temp_f is None or temp_f > max_temp_f:
            max_temp_f = temp_f
            occurrence_times = [valid]
        elif temp_f == max_temp_f:
            occurrence_times.append(valid)

    latest_valid = occurrence_times[-1] if occurrence_times else None

    return {
        "madis_high_f": max_temp_f,
        "occurrence_count": len(occurrence_times),
        "occurrence_times": occurrence_times,
        "latest_high_time": latest_valid,
        "row_count": total_rows,
    }


def get_state_key(target_sf_date_str: str) -> str:
    """Build S3 key for storing alert state per day."""
    return f"{MADIS_STATE_PREFIX.rstrip('/')}/{target_sf_date_str}.json"


def load_state(bucket: str, target_sf_date_str: str) -> dict:
    """
    Load previous alert state for a given day.

    Used to prevent duplicate alerts.
    """
    key = get_state_key(target_sf_date_str)
    try:
        return read_json_from_s3(bucket, key)
    except Exception:
        return {
            "date_sf": target_sf_date_str,
            "last_alerted_high_f": None,
            "last_alerted_occurrence_count": 0,
            "last_source_key": None,
        }


def save_state(bucket: str, target_sf_date_str: str, state: dict) -> None:
    """Persist updated alert state to S3."""
    write_json_to_s3(bucket, get_state_key(target_sf_date_str), state)


def should_alert(madis_high_f, occurrence_count, state):
    """
    Core alerting logic:

    - Alert on new higher MADIS high
    - Alert when a previously single-occurrence high repeats (confidence)
    - Avoid duplicate alerts for unchanged conditions
    """
    last_high = state.get("last_alerted_high_f")
    last_count = int(state.get("last_alerted_occurrence_count") or 0)

    if madis_high_f is None:
        return False, "no_madis_high"

    if last_high is None:
        return True, "first_alert"

    if madis_high_f > last_high:
        return True, "new_high"

    if madis_high_f == last_high and occurrence_count > last_count:
        if last_count == 1 and occurrence_count >= 2:
            return True, "high_tied_again"

    return False, "no_change"


def build_slack_message(
    source_key: str,
    target_sf_date_str: str,
    madis_high_f: int,
    occurrence_count: int,
    occurrence_times: list,
    metar_high_f,
    latest_high_time: str,
    reason: str,
    row_count: int,
):
    """
    Construct human-readable Slack alert message.
    """
    diff = "N/A"
    if isinstance(metar_high_f, int):
        diff = madis_high_f - metar_high_f

    pretty_times_pt = ", ".join(fmt_sf(t) for t in occurrence_times) if occurrence_times else "N/A"
    pretty_times_et = ", ".join(fmt_et(t) for t in occurrence_times) if occurrence_times else "N/A"

    if reason == "new_high":
        title = ":thermometer: *KSFO MADIS New High Above METAR*"
    elif reason == "high_tied_again":
        title = ":thermometer: *KSFO MADIS High Repeated Above METAR*"
    else:
        title = ":thermometer: *KSFO MADIS High Alert*"

    return (
        f"{title}\n"
        f"SF Local Day: {target_sf_date_str}\n"
        f"MADIS High: {madis_high_f}°F\n"
        f"METAR High So Far: {metar_high_f}°F\n"
        f"Difference: {diff}°F\n"
        f"Occurrences at MADIS High: {occurrence_count}\n"
        f"Latest High Time (PT): {fmt_sf(latest_high_time)}\n"
        f"Latest High Time (ET): {fmt_et(latest_high_time)}\n"
        f"All High Times (PT): {pretty_times_pt}\n"
        f"All High Times (ET): {pretty_times_et}\n"
        f"Reason: {reason}\n"
        f"Rows Scanned: {row_count}\n"
        f"Source File: `{source_key}`"
    )


def lambda_handler(event, context):
    """
    Lambda triggered by S3 uploads of MADIS CSV snapshots.

    Workflow:
      1. Read new CSV snapshot
      2. Compute MADIS daily high + occurrence pattern
      3. Compute METAR daily high (baseline)
      4. Compare + decide if alert should fire
      5. Send Slack alert + update state
    """
    processed = []

    for rec in event.get("Records", []):
        bucket = rec["s3"]["bucket"]["name"]
        key = urllib.parse.unquote_plus(rec["s3"]["object"]["key"])

        # Only process CSV snapshot files.
        if not key.endswith(".csv"):
            print(f"Skipping non-CSV object: s3://{bucket}/{key}")
            continue

        # Skip internal state files.
        if "/state/" in key:
            print(f"Skipping state object: s3://{bucket}/{key}")
            continue

        print(f"Processing s3://{bucket}/{key}")

        csv_text = read_text_from_s3(bucket, key)
        madis_stats = analyze_madis_csv(csv_text)

        # Extract date partition from S3 key.
        path_parts = key.split("/")
        target_sf_date_str = None
        for part in path_parts:
            if len(part) == 10 and part[4] == "-" and part[7] == "-":
                target_sf_date_str = part
                break
        if not target_sf_date_str:
            raise ValueError(f"Could not infer target date from key: {key}")

        target_sf_date = datetime.strptime(target_sf_date_str, "%Y-%m-%d").date()

        # Compute METAR baseline high.
        metar_stats = get_sf_day_metar_high(bucket, METAR_PREFIX, target_sf_date)

        madis_high_f = madis_stats["madis_high_f"]
        metar_high_f = metar_stats["high_f"]

        occurrence_count = madis_stats["occurrence_count"]
        occurrence_times = madis_stats["occurrence_times"]
        latest_high_time = madis_stats["latest_high_time"]
        row_count = madis_stats["row_count"]

        if madis_high_f is None:
            print(f"No MADIS high parsed from s3://{bucket}/{key}")
            continue

        if not isinstance(metar_high_f, int):
            print(f"No usable METAR high yet for {target_sf_date_str}; skipping alert")
            continue

        state = load_state(bucket, target_sf_date_str)

        # Allow repeat alert if confidence increases (2+ occurrences).
        allow_repeat_after_metar_catchup = (
            state.get("last_alerted_high_f") == madis_high_f and
            int(state.get("last_alerted_occurrence_count") or 0) == 1 and
            occurrence_count >= 2
        )

        # Only alert if MADIS exceeds METAR baseline (or special repeat case).
        if madis_high_f <= metar_high_f and not allow_repeat_after_metar_catchup:
            print(
                f"No alert: madis_high_f={madis_high_f} <= metar_high_f={metar_high_f}"
            )
            continue

        alert, reason = should_alert(madis_high_f, occurrence_count, state)

        if alert:
            message = build_slack_message(
                source_key=key,
                target_sf_date_str=target_sf_date_str,
                madis_high_f=madis_high_f,
                occurrence_count=occurrence_count,
                occurrence_times=occurrence_times,
                metar_high_f=metar_high_f,
                latest_high_time=latest_high_time,
                reason=reason,
                row_count=row_count,
            )
            post_to_slack(message)

            # Update state to prevent duplicate alerts.
            state.update(
                {
                    "date_sf": target_sf_date_str,
                    "last_alerted_high_f": madis_high_f,
                    "last_alerted_occurrence_count": occurrence_count,
                    "last_source_key": key,
                    "last_reason": reason,
                    "last_updated_at_utc": datetime.now(timezone.utc).isoformat(),
                    "latest_metar_high_f": metar_high_f,
                    "latest_madis_high_f": madis_high_f,
                    "latest_occurrence_times": occurrence_times,
                }
            )
            save_state(bucket, target_sf_date_str, state)

            print(f"Alert sent for {target_sf_date_str}: {reason}")

        else:
            print(f"No alert after state comparison: reason={reason}")

        processed.append(
            {
                "bucket": bucket,
                "key": key,
                "date_sf": target_sf_date_str,
                "madis_high_f": madis_high_f,
                "metar_high_f": metar_high_f,
                "occurrence_count": occurrence_count,
            }
        )

    return {
        "statusCode": 200,
        "body": json.dumps(
            {
                "message": "Processed S3 event",
                "processed_count": len(processed),
                "processed": processed,
            }
        ),
    }