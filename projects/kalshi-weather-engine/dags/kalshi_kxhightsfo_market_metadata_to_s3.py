from __future__ import annotations

import json
import logging
import re
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
# Local timezone used by Airflow for scheduling.
TZ = "America/Los_Angeles"

# Base API endpoint for Kalshi election/weather market data.
KALSHI_BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"

# Series ticker for the specific market family this DAG tracks.
SERIES_TICKER = "KXHIGHTSFO"

# Destination in S3 for storing one JSON file per event date.
S3_BUCKET = "weather-kalshi-data"
S3_PREFIX = f"raw/kalshi/historical/market/{SERIES_TICKER}"

# Request tuning.
REQUEST_TIMEOUT = 30
PAGE_LIMIT = 200

# Shared HTTP headers for Kalshi API requests.
HEADERS = {
    "Accept": "application/json",
    "User-Agent": "Mozilla/5.0 (compatible; Airflow-Kalshi-Market-Metadata/1.0)",
}

# Default Airflow retry behavior.
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
    Small wrapper around GET requests to the Kalshi API.

    Keeps URL construction, headers, timeout, and JSON parsing consistent
    across all API calls in this DAG.
    """
    url = f"{KALSHI_BASE_URL}{path}"
    resp = session.get(url, params=params or {}, headers=HEADERS, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    return resp.json()


def parse_event_date(event_obj: dict[str, Any]) -> str:
    """
    Derive the actual event date from the event ticker.

    Example:
        KXHIGHTSFO-26JAN31 -> 2026-01-31

    We do this because the event ticker contains the date in a stable format,
    which is more reliable for file naming and downstream processing.
    """
    event_ticker = event_obj.get("event_ticker", "")

    m = re.search(r"-(\d{2})([A-Z]{3})(\d{2})$", event_ticker)
    if not m:
        raise AirflowException(f"Could not determine event date for event_ticker={event_ticker}")

    year_2, mon_abbr, day = m.groups()

    month_map = {
        "JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
        "JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12,
    }

    year = 2000 + int(year_2)
    dt = pendulum.date(year, month_map[mon_abbr], int(day))
    return str(dt)


def build_s3_key(event_date: str) -> str:
    """
    Build the S3 object key for a single event-date snapshot.
    """
    return f"{S3_PREFIX}/{event_date}_{SERIES_TICKER}.json"


def event_is_finalized(document: dict[str, Any]) -> bool:
    """
    Return True only if every market in the event has status='finalized'.

    If no markets exist, treat the event as not finalized so it can be
    revisited later instead of being assumed complete.
    """
    markets = document.get("markets", [])
    if not markets:
        return False

    return all(str(m.get("status", "")).lower() == "finalized" for m in markets)


def inspect_recent_s3_state(s3_client) -> tuple[Optional[pendulum.Date], list[str]]:
    """
    Inspect what has already been stored in S3.

    Returns:
      - latest stored event date
      - list of recent stored event dates that should be refreshed

    Refresh logic:
      Starting from the newest stored file, walk backward until the first
      fully finalized event is found. Any newer non-finalized dates are
      candidates for refresh because market metadata may still change.
    """
    paginator = s3_client.get_paginator("list_objects_v2")
    stored: list[tuple[pendulum.Date, str]] = []

    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=f"{S3_PREFIX}/"):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            filename = key.rsplit("/", 1)[-1]

            # Expected filename format:
            # YYYY-MM-DD_<SERIES_TICKER>.json
            m = re.match(
                r"(\d{4}-\d{2}-\d{2})_" + re.escape(SERIES_TICKER) + r"\.json$",
                filename,
            )
            if not m:
                continue

            dt = pendulum.parse(m.group(1)).date()
            stored.append((dt, key))

    if not stored:
        return None, []

    # Newest files first so we can identify the current processing frontier.
    stored.sort(key=lambda x: x[0], reverse=True)
    latest_date = stored[0][0]

    refresh_dates: list[str] = []

    for dt, key in stored:
        body = s3_client.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read()
        document = json.loads(body)

        # Once we reach a fully finalized event, older files should also be stable,
        # so we can stop scanning.
        if event_is_finalized(document):
            break

        refresh_dates.append(str(dt))

    return latest_date, refresh_dates


def list_all_events_for_series(session: requests.Session) -> list[dict[str, Any]]:
    """
    Pull all events for the configured series from Kalshi.

    Uses pagination because a series can contain more events than a single
    API response returns.
    """
    cursor: Optional[str] = None
    events: list[dict[str, Any]] = []

    while True:
        params = {
            "limit": PAGE_LIMIT,
            "series_ticker": SERIES_TICKER,
            "with_nested_markets": "true",
        }
        if cursor:
            params["cursor"] = cursor

        payload = kalshi_get(session, "/events", params=params)
        batch = payload.get("events", [])
        events.extend(batch)

        cursor = payload.get("cursor")
        if not cursor:
            break

    return events


def get_event_with_markets(session: requests.Session, event_ticker: str) -> dict[str, Any]:
    """
    Fetch a single event and make sure market data is attached.

    Primary path:
        /events/{event_ticker}?with_nested_markets=true

    Fallback path:
        /historical/markets?event_ticker=...

    The fallback exists because nested markets are not always returned
    consistently from the event endpoint.
    """
    payload = kalshi_get(
        session,
        f"/events/{event_ticker}",
        params={"with_nested_markets": "true"},
    )

    event = payload.get("event", payload)
    markets = event.get("markets") or payload.get("markets") or []

    if not markets:
        hist = kalshi_get(
            session,
            "/historical/markets",
            params={"event_ticker": event_ticker, "limit": 1000},
        )
        markets = hist.get("markets", [])

    event["markets"] = markets
    return event


def simplify_market(mkt: dict[str, Any]) -> dict[str, Any]:
    """
    Keep only the market fields we actually want to persist.

    This makes stored JSON smaller, easier to inspect, and more stable for
    downstream consumers.
    """
    return {
        "ticker": mkt.get("ticker"),
        "event_ticker": mkt.get("event_ticker"),
        "status": mkt.get("status"),
        "result": mkt.get("result"),
        "title": mkt.get("title"),
        "subtitle": mkt.get("subtitle"),
        "yes_sub_title": mkt.get("yes_sub_title"),
        "no_sub_title": mkt.get("no_sub_title"),
        "strike_type": mkt.get("strike_type"),
        "floor_strike": mkt.get("floor_strike"),
        "cap_strike": mkt.get("cap_strike"),
        "functional_strike": mkt.get("functional_strike"),
        "custom_strike": mkt.get("custom_strike"),
        "open_time": mkt.get("open_time"),
        "close_time": mkt.get("close_time"),
        "expiration_time": mkt.get("expiration_time"),
        "latest_expiration_time": mkt.get("latest_expiration_time"),
        "settlement_ts": mkt.get("settlement_ts"),
        "settlement_value_dollars": mkt.get("settlement_value_dollars"),
        "yes_bid_dollars": mkt.get("yes_bid_dollars"),
        "yes_ask_dollars": mkt.get("yes_ask_dollars"),
        "no_bid_dollars": mkt.get("no_bid_dollars"),
        "no_ask_dollars": mkt.get("no_ask_dollars"),
        "last_price_dollars": mkt.get("last_price_dollars"),
        "volume_fp": mkt.get("volume_fp"),
        "open_interest_fp": mkt.get("open_interest_fp"),
    }


def build_event_document(event: dict[str, Any]) -> dict[str, Any]:
    """
    Convert a raw Kalshi event payload into the final document shape written to S3.

    In addition to copying core event metadata, this function computes a few
    convenience fields such as:
      - event_date
      - all_markets_finalized
      - winning_market_ticker
      - winning_market_subtitle
      - collected_at_utc
    """
    event_date = parse_event_date(event)
    markets = [simplify_market(m) for m in event.get("markets", [])]

    # In mutually exclusive markets, the winning contract is the one
    # whose result resolves to "yes".
    winning_markets = [m for m in markets if str(m.get("result", "")).lower() == "yes"]
    winning_market_ticker = winning_markets[0]["ticker"] if winning_markets else None
    winning_market_subtitle = (
        (winning_markets[0].get("subtitle") or winning_markets[0].get("yes_sub_title"))
        if winning_markets
        else None
    )

    all_markets_finalized = event_is_finalized({"markets": markets})

    return {
        "series_ticker": SERIES_TICKER,
        "event_date": event_date,
        "event_ticker": event.get("event_ticker"),
        "title": event.get("title"),
        "sub_title": event.get("sub_title"),
        "category": event.get("category"),
        "mutually_exclusive": event.get("mutually_exclusive"),
        "strike_date": event.get("strike_date"),
        "strike_period": event.get("strike_period"),
        "status": event.get("status"),
        "open_time": event.get("open_time"),
        "close_time": event.get("close_time"),
        "settlement_status": event.get("settlement_status"),
        "settlement_sources": event.get("settlement_sources"),
        "all_markets_finalized": all_markets_finalized,
        "markets": markets,
        "winning_market_ticker": winning_market_ticker,
        "winning_market_subtitle": winning_market_subtitle,
        "collected_at_utc": pendulum.now("UTC").to_iso8601_string(),
    }


# =========================================================
# DAG
# =========================================================
@dag(
    dag_id="kalshi_kxhightsfo_market_metadata_to_s3",
    start_date=pendulum.datetime(2026, 3, 1, tz=TZ),
    schedule="0 8 * * *",
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["kalshi", "weather", "market-metadata", SERIES_TICKER.lower()],
)
def kalshi_kxhightsfo_market_metadata_to_s3():
    """
    Daily DAG that syncs Kalshi event/market metadata for a single series into S3.

    Processing strategy:
      1. Inspect what has already been stored.
      2. Refresh recent non-finalized dates.
      3. Ingest any brand-new event dates not seen before.
      4. Write one JSON document per event date to S3.
    """

    @task()
    def determine_processing_state() -> dict[str, Any]:
        """
        Figure out where the pipeline left off.

        We use this to avoid reprocessing the full history every day while still
        refreshing the newest events that may not be finalized yet.
        """
        s3 = boto3.client("s3")
        latest, refresh_dates = inspect_recent_s3_state(s3)

        state = {
            "latest_stored_date": str(latest) if latest else None,
            "refresh_dates": refresh_dates,
        }

        logging.info("Processing state: %s", state)
        return state

    @task()
    def fetch_events_to_process(state: dict[str, Any]) -> list[dict[str, Any]]:
        """
        Decide which events should be fetched on this run.

        We process:
          - everything on the first run
          - events newer than the latest stored date
          - recent stored dates that are not finalized yet
        """
        session = requests.Session()
        all_events = list_all_events_for_series(session)

        if not all_events:
            raise AirflowException(f"No events found for series {SERIES_TICKER}")

        latest_stored_date = state.get("latest_stored_date")
        refresh_dates = set(state.get("refresh_dates", []))

        cutoff = pendulum.parse(latest_stored_date).date() if latest_stored_date else None

        selected: list[dict[str, Any]] = []

        for event in all_events:
            try:
                event_date_str = parse_event_date(event)
                event_date = pendulum.parse(event_date_str).date()
            except Exception as exc:
                logging.warning(
                    "Skipping event due to date parse failure: %s ; event=%s",
                    exc,
                    event,
                )
                continue

            should_process = False

            if cutoff is None:
                # First run: backfill the entire available history.
                should_process = True
            elif event_date > cutoff:
                # New event date not stored yet.
                should_process = True
            elif event_date_str in refresh_dates:
                # Existing recent file is still not finalized, so refresh it.
                should_process = True

            if should_process:
                selected.append(
                    {
                        "event_ticker": event["event_ticker"],
                        "event_date": event_date_str,
                    }
                )

        selected = sorted(selected, key=lambda x: x["event_date"])
        logging.info("Selected %s event(s) to process", len(selected))
        return selected

    @task()
    def fetch_and_store_metadata(events_to_process: list[dict[str, Any]]) -> list[str]:
        """
        Fetch full metadata for each selected event and write it to S3.

        Returns a list of written S3 URIs for easier debugging / task inspection.
        """
        if not events_to_process:
            logging.info("No event dates to process.")
            return []

        session = requests.Session()
        s3 = boto3.client("s3")
        written: list[str] = []

        for item in events_to_process:
            event_ticker = item["event_ticker"]

            event = get_event_with_markets(session, event_ticker)
            document = build_event_document(event)
            event_date = document["event_date"]
            key = build_s3_key(event_date)

            s3.put_object(
                Bucket=S3_BUCKET,
                Key=key,
                Body=json.dumps(document, indent=2).encode("utf-8"),
                ContentType="application/json",
            )

            s3_uri = f"s3://{S3_BUCKET}/{key}"
            written.append(s3_uri)
            logging.info("Wrote %s", s3_uri)

        return written

    state = determine_processing_state()
    to_process = fetch_events_to_process(state)
    fetch_and_store_metadata(to_process)


dag = kalshi_kxhightsfo_market_metadata_to_s3()