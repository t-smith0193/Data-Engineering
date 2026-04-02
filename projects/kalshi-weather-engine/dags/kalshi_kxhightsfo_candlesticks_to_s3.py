from __future__ import annotations

import json
import logging
import re
import time
from datetime import timedelta
from typing import Any, Optional

import boto3
import pendulum
import requests
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException

# =========================================================
# CONFIG
# =========================================================
# Local timezone used for day-boundary calculations and Airflow scheduling.
TZ = "America/Los_Angeles"

# Base API endpoint for Kalshi market data.
KALSHI_BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"

# Series this DAG is responsible for.
SERIES_TICKER = "KXHIGHTSFO"

# S3 locations for previously stored market metadata and candlestick payloads.
S3_BUCKET = "weather-kalshi-data"
MARKET_PREFIX = f"raw/kalshi/historical/market/{SERIES_TICKER}"
CANDLE_PREFIX = "raw/kalshi/historical/candlestick/KXHIGHTSFO"

# HTTP and rate-limit tuning.
REQUEST_TIMEOUT = 30
MAX_HTTP_RETRIES = 4
RATE_LIMIT_SLEEP_SECONDS = 0.25

# Shared request headers for Kalshi API calls.
HEADERS = {
    "Accept": "application/json",
    "User-Agent": "Mozilla/5.0 (compatible; Airflow-Kalshi-Candlesticks/1.0)",
}

# Default Airflow retry behavior for task failures.
DEFAULT_ARGS = {
    "owner": "tyler",
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}


# =========================================================
# HELPERS
# =========================================================
def kalshi_get(
    session: requests.Session,
    path: str,
    params: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    """
    Execute a GET request against the Kalshi API with basic retry handling.

    This helper specifically retries on HTTP 429 responses, which are expected
    when pulling a large number of market candlestick payloads in sequence.
    """
    url = f"{KALSHI_BASE_URL}{path}"

    for attempt in range(1, MAX_HTTP_RETRIES + 1):
        resp = session.get(url, params=params or {}, headers=HEADERS, timeout=REQUEST_TIMEOUT)

        if resp.status_code == 429:
            # Back off progressively when rate limited rather than failing fast.
            sleep_seconds = attempt * 2
            logging.warning(
                "Rate limited on %s params=%s attempt=%s/%s; sleeping %ss",
                path,
                params,
                attempt,
                MAX_HTTP_RETRIES,
                sleep_seconds,
            )
            time.sleep(sleep_seconds)
            continue

        resp.raise_for_status()
        return resp.json()

    raise AirflowException(f"Exceeded retry limit for Kalshi request: {url} params={params}")


def list_metadata_keys(s3_client) -> list[str]:
    """
    List all stored event metadata files for this series.
    """
    paginator = s3_client.get_paginator("list_objects_v2")
    keys: list[str] = []

    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=f"{MARKET_PREFIX}/"):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])

    return keys


def list_candle_keys(s3_client) -> list[str]:
    """
    List all stored candlestick files for this series.
    """
    paginator = s3_client.get_paginator("list_objects_v2")
    keys: list[str] = []

    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=f"{CANDLE_PREFIX}/"):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])

    return keys


def extract_date_from_metadata_key(key: str) -> Optional[str]:
    """
    Extract the event date from a metadata object key.

    Expected format:
        .../YYYY-MM-DD_<SERIES_TICKER>.json
    """
    filename = key.rsplit("/", 1)[-1]
    m = re.match(r"(\d{4}-\d{2}-\d{2})_" + re.escape(SERIES_TICKER) + r"\.json$", filename)
    return m.group(1) if m else None


def extract_date_from_candle_key(key: str) -> Optional[str]:
    """
    Extract the event date from a candlestick object key.

    Expected format:
        .../candlestick/<series>/<YYYY-MM-DD>/<market_file>.json
    """
    m = re.search(rf"{re.escape(CANDLE_PREFIX)}/(\d{{4}}-\d{{2}}-\d{{2}})/", key)
    return m.group(1) if m else None


def build_market_filename(market_ticker: str) -> str:
    """
    Convert a full market ticker into a shorter, cleaner filename.

    Examples:
        KXHIGHTSFO-26MAR23-T80   -> T80.json
        KXHIGHTSFO-26MAR23-B80.5 -> B80.5.json

    If the ticker does not match the expected pattern, fall back to using the
    full ticker so the write still succeeds.
    """
    pattern = rf"^{re.escape(SERIES_TICKER)}-\d{{2}}[A-Z]{{3}}\d{{2}}-(.+)$"
    m = re.match(pattern, market_ticker)
    suffix = m.group(1) if m else market_ticker
    return f"{suffix}.json"


def build_candle_key(event_date: str, market_ticker: str) -> str:
    """
    Build the S3 object key for a single market's candlestick payload.
    """
    filename = build_market_filename(market_ticker)
    return f"{CANDLE_PREFIX}/{event_date}/{filename}"


def latest_candle_date(s3_client) -> Optional[pendulum.Date]:
    """
    Return the most recent event date that already has candlestick data in S3.
    """
    candle_keys = list_candle_keys(s3_client)
    dates: list[pendulum.Date] = []

    for key in candle_keys:
        event_date = extract_date_from_candle_key(key)
        if event_date:
            dates.append(pendulum.parse(event_date).date())

    return max(dates) if dates else None


def determine_metadata_refresh_dates(s3_client) -> list[str]:
    """
    Identify recent metadata dates that still need candlestick refreshes.

    Starting from the newest metadata file, walk backward until the first
    fully finalized event is found. More recent non-finalized dates are
    returned so their candlesticks can be pulled again on future DAG runs.
    """
    metadata_keys = list_metadata_keys(s3_client)
    dated_keys: list[tuple[pendulum.Date, str]] = []

    for key in metadata_keys:
        event_date = extract_date_from_metadata_key(key)
        if not event_date:
            continue
        dated_keys.append((pendulum.parse(event_date).date(), key))

    if not dated_keys:
        return []

    # Work backward from the newest dates because these are the ones most likely
    # to still be changing.
    dated_keys.sort(key=lambda x: x[0], reverse=True)
    refresh_dates: list[str] = []

    for dt, key in dated_keys:
        obj = s3_client.get_object(Bucket=S3_BUCKET, Key=key)
        document = json.loads(obj["Body"].read())

        # Once a date is fully finalized, older dates should already be stable.
        if bool(document.get("all_markets_finalized", False)):
            break

        refresh_dates.append(str(dt))

    return refresh_dates


def event_day_bounds_utc(event_date: str) -> tuple[int, int]:
    """
    Convert an event date into UTC Unix timestamps spanning that full local day.

    The Kalshi candlestick endpoint expects UTC timestamps, but the event itself
    is defined in America/Los_Angeles calendar time, so we translate the local
    day boundaries into UTC before making the API request.
    """
    day_start_local = pendulum.parse(event_date, tz=TZ).start_of("day")
    day_end_local = pendulum.parse(event_date, tz=TZ).end_of("day")

    start_ts = int(day_start_local.in_timezone("UTC").timestamp())
    end_ts = int(day_end_local.in_timezone("UTC").timestamp())

    return start_ts, end_ts


def fetch_market_candlesticks(
    session: requests.Session,
    market_ticker: str,
    event_date: str,
) -> dict[str, Any]:
    """
    Fetch one-minute candlestick data for a single market over its event day.
    """
    start_ts, end_ts = event_day_bounds_utc(event_date)

    return kalshi_get(
        session,
        f"/series/{SERIES_TICKER}/markets/{market_ticker}/candlesticks",
        params={
            "start_ts": start_ts,
            "end_ts": end_ts,
            "period_interval": 1,
        },
    )


# =========================================================
# DAG
# =========================================================
@dag(
    dag_id="kalshi_kxhightsfo_candlesticks_to_s3",
    start_date=pendulum.datetime(2026, 3, 1, tz=TZ),
    schedule="30 8 * * *",
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["kalshi", "candlesticks", SERIES_TICKER.lower()],
)
def kalshi_kxhightsfo_candlesticks_to_s3():
    """
    Daily DAG that pulls market candlestick data and stores it in S3.

    Processing strategy:
      1. Find the latest candlestick date already stored.
      2. Revisit recent metadata dates that are not fully finalized.
      3. For each selected event date, fetch candlesticks for every market.
      4. Write one file per market under a date-partitioned S3 path.
    """

    @task()
    def determine_processing_state() -> dict[str, Any]:
        """
        Determine the current candlestick ingestion boundary and refresh window.
        """
        s3 = boto3.client("s3")

        latest_candle_dt = latest_candle_date(s3)
        refresh_dates = determine_metadata_refresh_dates(s3)

        state = {
            "latest_candle_date": str(latest_candle_dt) if latest_candle_dt else None,
            "refresh_dates": refresh_dates,
        }

        logging.info("Candlestick processing state: %s", state)
        return state

    @task()
    def select_metadata_to_process(state: dict[str, Any]) -> list[dict[str, Any]]:
        """
        Select which metadata documents should drive candlestick collection.

        We process:
          - all metadata dates on first run
          - dates newer than the latest stored candlestick date
          - recent dates whose metadata is still not finalized
        """
        s3 = boto3.client("s3")
        metadata_keys = list_metadata_keys(s3)

        if not metadata_keys:
            raise AirflowException("No metadata files found in S3")

        latest_candle_date_str = state.get("latest_candle_date")
        refresh_dates = set(state.get("refresh_dates", []))

        candle_cutoff = (
            pendulum.parse(latest_candle_date_str).date()
            if latest_candle_date_str
            else None
        )

        selected: list[dict[str, Any]] = []

        for key in metadata_keys:
            event_date = extract_date_from_metadata_key(key)
            if not event_date:
                continue

            event_dt = pendulum.parse(event_date).date()

            should_process = False

            if candle_cutoff is None:
                # First run: process all available metadata dates.
                should_process = True
            elif event_dt > candle_cutoff:
                # Newer metadata date not yet represented in candlestick storage.
                should_process = True
            elif event_date in refresh_dates:
                # Metadata is still unsettled, so candlestick data may need another pull.
                should_process = True

            if should_process:
                obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
                document = json.loads(obj["Body"].read())

                selected.append(
                    {
                        "event_date": document["event_date"],
                        "markets": document.get("markets", []),
                        "all_markets_finalized": document.get("all_markets_finalized", False),
                    }
                )

        selected = sorted(selected, key=lambda x: x["event_date"])
        logging.info("Selected %s metadata day(s) to process for candlesticks", len(selected))
        return selected

    @task()
    def fetch_and_store_candlesticks(selected_metadata: list[dict[str, Any]]) -> list[str]:
        """
        Fetch candlestick payloads for every market in each selected metadata day
        and write them to S3.

        Failures are logged per market so one bad response does not stop the
        rest of the day from being processed.
        """
        if not selected_metadata:
            logging.info("No candlestick dates to process.")
            return []

        session = requests.Session()
        s3 = boto3.client("s3")
        written: list[str] = []

        for record in selected_metadata:
            event_date = record["event_date"]
            markets = record.get("markets", [])

            for market in markets:
                market_ticker = market.get("ticker")
                if not market_ticker:
                    continue

                try:
                    payload = fetch_market_candlesticks(
                        session=session,
                        market_ticker=market_ticker,
                        event_date=event_date,
                    )

                    key = build_candle_key(event_date, market_ticker)

                    s3.put_object(
                        Bucket=S3_BUCKET,
                        Key=key,
                        Body=json.dumps(payload, indent=2).encode("utf-8"),
                        ContentType="application/json",
                    )

                    s3_uri = f"s3://{S3_BUCKET}/{key}"
                    written.append(s3_uri)
                    logging.info("Wrote %s", s3_uri)

                    # Small delay between requests helps avoid unnecessary 429s.
                    time.sleep(RATE_LIMIT_SLEEP_SECONDS)

                except Exception as exc:
                    logging.warning(
                        "Failed candlestick pull for event_date=%s market_ticker=%s error=%s",
                        event_date,
                        market_ticker,
                        exc,
                    )
                    # Keep the same pacing even on failures to avoid hammering the API.
                    time.sleep(RATE_LIMIT_SLEEP_SECONDS)

        return written

    state = determine_processing_state()
    selected = select_metadata_to_process(state)
    fetch_and_store_candlesticks(selected)


dag = kalshi_kxhightsfo_candlesticks_to_s3()