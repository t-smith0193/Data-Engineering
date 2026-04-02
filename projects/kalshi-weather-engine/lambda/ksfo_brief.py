import os
import json
import boto3
import urllib.request
import urllib.parse
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

# Reuse AWS clients across Lambda invocations for better efficiency.
s3 = boto3.client("s3")
lambda_client = boto3.client("lambda")

# Environment configuration.
SLACK_WEBHOOK_URL = os.environ["SLACK_WEBHOOK_URL"]
METAR_PREFIX = os.environ["METAR_PREFIX"]
ANALYSIS_LAMBDA_NAME = os.environ["ANALYSIS_LAMBDA_NAME"]

# Optional overrides for where downstream analysis parquet files live.
PROCESSED_MARKET_PREFIX = os.environ.get("PROCESSED_MARKET_PREFIX", "processed/current_day_market/")
ANALYSIS_BUCKET = os.environ.get("ANALYSIS_BUCKET", "")

# Timezones used for user-facing timestamps.
ET_TZ = ZoneInfo("America/New_York")
SF_TZ = ZoneInfo("America/Los_Angeles")


def c_to_f(c):
    """
    Convert Celsius to Fahrenheit using round-half-up style whole-degree output.

    Returns "N/A" for missing or invalid values.
    """
    try:
        c = float(c)
    except (TypeError, ValueError):
        return "N/A"

    return int((c * 9 / 5) + 32 + 0.5)


def fmt_epoch_utc(epoch_value):
    """
    Format epoch seconds as a UTC timestamp string.
    """
    if epoch_value in (None, ""):
        return "N/A"
    return datetime.fromtimestamp(epoch_value, tz=timezone.utc).strftime("%Y-%m-%d %H:%MZ")


def fmt_epoch_et(epoch_value):
    """
    Format epoch seconds as Eastern Time.
    """
    if epoch_value in (None, ""):
        return "N/A"
    dt = datetime.fromtimestamp(epoch_value, tz=timezone.utc)
    return dt.astimezone(ET_TZ).strftime("%Y-%m-%d %I:%M %p ET")


def fmt_epoch_sf(epoch_value):
    """
    Format epoch seconds as Pacific Time (San Francisco local time).
    """
    if epoch_value in (None, ""):
        return "N/A"
    dt = datetime.fromtimestamp(epoch_value, tz=timezone.utc)
    return dt.astimezone(SF_TZ).strftime("%Y-%m-%d %I:%M %p PT")


def fmt_iso_utc(value):
    """
    Format ISO timestamp string as UTC.
    """
    if not value:
        return "N/A"
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return dt.strftime("%Y-%m-%d %H:%MZ")
    except Exception:
        return str(value)


def fmt_iso_et(value):
    """
    Format ISO timestamp string as Eastern Time.
    """
    if not value:
        return "N/A"
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return dt.astimezone(ET_TZ).strftime("%Y-%m-%d %I:%M %p ET")
    except Exception:
        return str(value)


def fmt_iso_sf(value):
    """
    Format ISO timestamp string as Pacific Time.
    """
    if not value:
        return "N/A"
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return dt.astimezone(SF_TZ).strftime("%Y-%m-%d %I:%M %p PT")
    except Exception:
        return str(value)


def epoch_to_sf_date(epoch_value):
    """
    Convert epoch seconds into the corresponding SF-local calendar date.
    """
    if epoch_value in (None, ""):
        return None
    dt = datetime.fromtimestamp(epoch_value, tz=timezone.utc)
    return dt.astimezone(SF_TZ).date()


def read_json_from_s3(bucket: str, key: str) -> dict:
    """
    Read and parse a JSON object from S3.
    """
    resp = s3.get_object(Bucket=bucket, Key=key)
    return json.loads(resp["Body"].read().decode("utf-8"))


def post_to_slack(message: str) -> None:
    """
    Send a formatted Slack message via webhook.
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


def list_metar_keys_newest_first(bucket: str, prefix: str):
    """
    List METAR JSON files in reverse chronological order.

    Filenames begin with UTC ingest timestamp, so lexical descending order
    corresponds to newest pipeline-run files first.
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


def get_sf_day_high_low(bucket: str, prefix: str, target_sf_date):
    """
    Compute the current SF-local-day high and low temperature from METAR files.

    Strategy:
      - Scan newest METAR files backward
      - Use obsTime converted to America/Los_Angeles as the true date filter
      - Stop once we have already collected records for the target SF date
        and then encounter an older SF date

    This keeps the scan efficient while still respecting local calendar day
    boundaries.
    """
    high_c = None
    low_c = None
    matched_count = 0

    keys = list_metar_keys_newest_first(bucket, prefix)

    for key in keys:
        try:
            data = read_json_from_s3(bucket, key)

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
                if low_c is None or temp_c < low_c:
                    low_c = temp_c

            elif matched_count > 0 and obs_sf_date < target_sf_date:
                # Once we have data for the target day and then cross into an
                # older SF-local day, we can stop scanning.
                break

        except Exception as e:
            print(f"Skipping {key}: {e}")

    return {
        "high_c": round(high_c) if high_c is not None else None,
        "high_f": c_to_f(high_c) if high_c is not None else "N/A",
        "low_c": round(low_c) if low_c is not None else None,
        "low_f": c_to_f(low_c) if low_c is not None else "N/A",
        "date_sf": str(target_sf_date) if target_sf_date is not None else "N/A",
        "matched_count": matched_count,
    }


def build_sf_day_range_section(day_stats: dict) -> str:
    """
    Build a Slack-friendly summary of the SF-local-day temperature range so far.
    """
    high_c = day_stats.get("high_c")
    high_f = day_stats.get("high_f")
    low_c = day_stats.get("low_c")
    low_f = day_stats.get("low_f")
    date_sf = day_stats.get("date_sf")
    matched_count = day_stats.get("matched_count", 0)

    if high_c is None or low_c is None:
        return f"*SF Local Day Range So Far ({date_sf}):* Not enough METAR data yet"

    return (
        f"*SF Local Day Range So Far ({date_sf}):*\n"
        f"High: {high_c}°C / {high_f}°F\n"
        f"Low: {low_c}°C / {low_f}°F\n"
        f"Matched METAR Obs Count: {matched_count}"
    )


def build_metar_message_from_stats(key: str, record: dict, day_stats: dict) -> str:
    """
    Build the Slack message for a METAR-triggered update.

    Includes:
      - latest observation details
      - day high/low so far
      - raw METAR text
    """
    temp_c = record.get("temp", "N/A")
    temp_f = c_to_f(temp_c)

    dewp_c = record.get("dewp", "N/A")
    dewp_f = c_to_f(dewp_c)

    obs_time = record.get("obsTime")
    day_range_section = build_sf_day_range_section(day_stats)

    return (
        ":cloud: *KSFO METAR Update*\n"
        f"Trigger: METAR\n"
        f"Obs Time (UTC): {fmt_epoch_utc(obs_time)}\n"
        f"Obs Time (ET): {fmt_epoch_et(obs_time)}\n"
        f"Obs Time (PT): {fmt_epoch_sf(obs_time)}\n"
        f"Temp: {temp_c}°C / {temp_f}°F\n"
        f"Dewpoint: {dewp_c}°C / {dewp_f}°F\n"
        f"Wind: {record.get('wdir', 'N/A')}° @ {record.get('wspd', 'N/A')} kt\n"
        f"Flight Category: {record.get('fltCat', 'N/A')}\n\n"
        f"{day_range_section}\n\n"
        f"Raw: `{record.get('rawOb', 'N/A')}`\n"
        f"File: `{key}`"
    )


def build_taf_message(key: str, data: dict) -> str:
    """
    Build the Slack message for a TAF-triggered update.
    """
    record = data["records"][0]

    return (
        ":airplane_departure: *KSFO TAF Update*\n"
        f"Trigger: TAF\n"
        f"Issue Time (UTC): {fmt_iso_utc(record.get('issueTime'))}\n"
        f"Issue Time (ET): {fmt_iso_et(record.get('issueTime'))}\n"
        f"Issue Time (PT): {fmt_iso_sf(record.get('issueTime'))}\n"
        f"Valid From (UTC): {fmt_epoch_utc(record.get('validTimeFrom'))}\n"
        f"Valid From (ET): {fmt_epoch_et(record.get('validTimeFrom'))}\n"
        f"Valid To (UTC): {fmt_epoch_utc(record.get('validTimeTo'))}\n"
        f"Valid To (ET): {fmt_epoch_et(record.get('validTimeTo'))}\n"
        f"Raw: `{record.get('rawTAF', 'N/A')}`\n"
        f"File: `{key}`"
    )


def build_unknown_message(key: str, data: dict) -> str:
    """
    Build a fallback Slack message for unrecognized weather file types.
    """
    product = data.get("product", "unknown")
    return (
        ":warning: *KSFO Weather File Received*\n"
        f"Trigger: UNKNOWN\n"
        f"Product: {product}\n"
        f"File: `{key}`"
    )


def build_analysis_payload(
    source_bucket: str,
    source_key: str,
    record: dict,
    day_stats: dict,
) -> dict:
    """
    Build the payload sent to the downstream analysis Lambda.

    This packages:
      - the current METAR observation
      - SF-local-day high/low context
      - the parquet path for the current day's market lookup
    """
    obs_time = record.get("obsTime")
    sf_date = epoch_to_sf_date(obs_time)
    market_date = str(sf_date) if sf_date is not None else None

    analysis_bucket = ANALYSIS_BUCKET or source_bucket
    parquet_key = f"{PROCESSED_MARKET_PREFIX.rstrip('/')}/{market_date}.parquet"

    return {
        "bucket": analysis_bucket,
        "parquet_key": parquet_key,
        "market_date": market_date,
        "current_day_high_f": day_stats.get("high_f"),
        "current_day_high_c": day_stats.get("high_c"),
        "current_day_low_f": day_stats.get("low_f"),
        "current_day_low_c": day_stats.get("low_c"),
        "matched_count": day_stats.get("matched_count"),
        "source_metar_bucket": source_bucket,
        "source_metar_key": source_key,
        "obs_time": obs_time,
        "obs_time_utc": fmt_epoch_utc(obs_time),
        "obs_time_et": fmt_epoch_et(obs_time),
        "obs_time_pt": fmt_epoch_sf(obs_time),
        "latest_temp_f": c_to_f(record.get("temp")),
        "latest_temp_c": record.get("temp"),
        "latest_dewp_f": c_to_f(record.get("dewp")),
        "latest_dewp_c": record.get("dewp"),
        "wind_dir_deg": record.get("wdir"),
        "wind_speed_kt": record.get("wspd"),
        "flight_category": record.get("fltCat"),
        "raw_metar": record.get("rawOb"),
    }


def invoke_ai_analysis(payload: dict) -> None:
    """
    Invoke the downstream analysis Lambda asynchronously.

    InvocationType="Event" makes this fire-and-forget so the current weather
    alert flow does not block on the downstream analysis execution.
    """
    response = lambda_client.invoke(
        FunctionName=ANALYSIS_LAMBDA_NAME,
        InvocationType="Event",
        Payload=json.dumps(payload).encode("utf-8"),
    )

    status_code = response.get("StatusCode")
    if status_code not in (202,):
        raise RuntimeError(f"Async Lambda invoke returned unexpected status code: {status_code}")


def lambda_handler(event, context):
    """
    Main Lambda entrypoint.

    Workflow:
      1. Trigger on S3 JSON weather files
      2. Detect product type (METAR / TAF / unknown)
      3. Post a Slack update
      4. For METAR files, also invoke downstream AI analysis asynchronously
    """
    processed = []

    for rec in event.get("Records", []):
        bucket = rec["s3"]["bucket"]["name"]
        key = urllib.parse.unquote_plus(rec["s3"]["object"]["key"])

        # Only process JSON payloads.
        if not key.endswith(".json"):
            print(f"Skipping non-JSON object: s3://{bucket}/{key}")
            continue

        print(f"Processing s3://{bucket}/{key}")
        data = read_json_from_s3(bucket, key)
        product = str(data.get("product", "")).lower()

        if product == "metar":
            records = data.get("records", [])
            if not records:
                print(f"No records found in METAR file: s3://{bucket}/{key}")
                continue

            record = records[0]
            obs_time = record.get("obsTime")
            sf_date = epoch_to_sf_date(obs_time)

            # Compute same-day high/low context for the Slack alert and analysis.
            day_stats = get_sf_day_high_low(bucket, METAR_PREFIX, sf_date)

            message = build_metar_message_from_stats(key, record, day_stats)
            post_to_slack(message)

            analysis_payload = build_analysis_payload(
                source_bucket=bucket,
                source_key=key,
                record=record,
                day_stats=day_stats,
            )

            try:
                invoke_ai_analysis(analysis_payload)
                print(
                    f"Invoked analysis lambda '{ANALYSIS_LAMBDA_NAME}' "
                    f"for market_date={analysis_payload.get('market_date')} "
                    f"parquet_key={analysis_payload.get('parquet_key')}"
                )
            except Exception as e:
                # Keep the primary Slack alert successful even if downstream
                # analysis fails.
                print(f"Failed to invoke analysis lambda for s3://{bucket}/{key}: {e}")

        elif product == "taf":
            records = data.get("records", [])
            if not records:
                print(f"No records found in TAF file: s3://{bucket}/{key}")
                continue

            message = build_taf_message(key, data)
            post_to_slack(message)

        else:
            # Fallback for unexpected product types so nothing is silently ignored.
            message = build_unknown_message(key, data)
            post_to_slack(message)

        processed.append(
            {
                "bucket": bucket,
                "key": key,
                "product": product or "unknown",
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