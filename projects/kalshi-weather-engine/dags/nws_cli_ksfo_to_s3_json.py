from __future__ import annotations

import json
import logging
import re
from collections import defaultdict
from datetime import timedelta
from typing import Any, Optional

import boto3
import pendulum
import requests
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from bs4 import BeautifulSoup

# =========================================================
# CONFIG
# =========================================================
# Local timezone used for interpreting report timestamps.
TZ = "America/Los_Angeles"

# NWS CLI product endpoint parameters.
BASE_URL = "https://forecast.weather.gov/product.php"
SITE = "MTR"
ISSUEDBY = "SFO"
PRODUCT = "CLI"
FORMAT = "TXT"
GLOSSARY = "0"

# S3 destination for parsed JSON outputs.
S3_BUCKET = "weather-kalshi-data"
S3_PREFIX = "raw/nws/cli/KSFO"

# Number of historical "versions" to scan on the NWS site.
MAX_VERSIONS_TO_SCAN = 50
REQUEST_TIMEOUT = 30

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; Airflow-NWS-CLI-Scraper/1.0)"
}

DEFAULT_ARGS = {
    "owner": "tyler",
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}

# Mapping of month abbreviations used in NWS reports.
MONTH_MAP = {
    "JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
    "JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12
}


# =========================================================
# GENERAL HELPERS
# =========================================================
def normalize_whitespace(text: str) -> str:
    """Collapse repeated spaces/tabs into a single space."""
    return re.sub(r"[ \t]+", " ", text).strip()


def raw_value_with_code(val: str) -> dict[str, Any]:
    """
    Parse raw numeric values that may include special NWS codes:
      - MM = missing
      - T = trace amount
    """
    val = val.strip()
    if val == "MM":
        return {"value": None, "code": "MM"}
    if val == "T":
        return {"value": None, "code": "T"}
    try:
        return {"value": float(val), "code": None}
    except ValueError:
        return {"value": None, "code": val}


def compact_time_to_hhmm(time_str: str, ampm: str) -> Optional[str]:
    """
    Convert compact or colon-separated time + AM/PM into 24h HH:MM format.
    Handles inputs like:
      - "530" + PM
      - "5:30" + PM
    """
    time_str = time_str.strip()
    ampm = ampm.strip().upper()

    if ":" in time_str:
        hh, mm = time_str.split(":")
        hour = int(hh)
        minute = int(mm)
    else:
        raw = time_str.zfill(4)
        hour = int(raw[:-2])
        minute = int(raw[-2:])

    if ampm == "AM":
        hour = 0 if hour == 12 else hour
    elif ampm == "PM":
        hour = 12 if hour == 12 else hour + 12
    else:
        return None

    return f"{hour:02d}:{minute:02d}"


def extract_pre_block_text(html: str) -> str:
    """
    Extract the raw report text from the <pre> block on the NWS page.

    Falls back to full page text if <pre> is missing.
    """
    soup = BeautifulSoup(html, "html.parser")
    pre = soup.find("pre")
    if pre:
        return pre.get_text("\n").strip() + "\n"

    text = soup.get_text("\n")
    lines = [line.rstrip() for line in text.splitlines()]
    return "\n".join(lines).strip() + "\n"


def fetch_version_html(session: requests.Session, version: int) -> str:
    """
    Fetch a specific version of the CLI report from NWS.

    Versions represent revisions of the same day's report.
    """
    params = {
        "site": SITE,
        "issuedby": ISSUEDBY,
        "product": PRODUCT,
        "format": FORMAT,
        "version": version,
        "glossary": GLOSSARY,
    }
    resp = session.get(BASE_URL, params=params, headers=HEADERS, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    return resp.text


def parse_issue_timestamp(report_text: str) -> Optional[pendulum.DateTime]:
    """
    Extract the report issue timestamp from the header.

    This determines whether a report is preliminary or finalized.
    """
    m = re.search(
        r"\n\s*(\d{3,4})\s+(AM|PM)\s+(PST|PDT)\s+\w{3}\s+([A-Z]{3})\s+(\d{1,2})\s+(\d{4})",
        report_text,
    )
    if not m:
        return None

    hhmm_raw, ampm, _, mon_abbr, day, year = m.groups()
    hhmm = hhmm_raw.zfill(4)

    hour12 = int(hhmm[:-2])
    minute = int(hhmm[-2:])

    if ampm == "AM":
        hour24 = 0 if hour12 == 12 else hour12
    else:
        hour24 = 12 if hour12 == 12 else hour12 + 12

    month = MONTH_MAP[mon_abbr.upper()]
    return pendulum.datetime(int(year), month, int(day), hour24, minute, tz=TZ)


def parse_summary_date(report_text: str) -> Optional[pendulum.Date]:
    """
    Extract the summary date (the day the report is about).
    """
    m = re.search(
        r"CLIMATE SUMMARY FOR\s+([A-Z]+)\s+(\d{1,2})\s+(\d{4})",
        report_text
    )
    if not m:
        return None

    month_name, day, year = m.groups()
    month_name = month_name.capitalize()

    dt = pendulum.from_format(f"{month_name} {day} {year}", "MMMM D YYYY", tz=TZ)
    return dt.date()


def is_5pm_preliminary(report_text: str) -> bool:
    """
    Detect if this is the 5PM preliminary report (not final).
    """
    return "VALID TODAY AS OF 0500 PM LOCAL TIME" in report_text.upper()


def choose_best_candidate(candidates: list[dict[str, Any]]) -> dict[str, Any]:
    """
    Select the canonical report for a given summary_date.

    Preference:
      1. Reports issued after the summary date (finalized)
      2. Otherwise, latest available version
    """
    if not candidates:
        raise AirflowException("No candidates provided")

    finalized = [
        c for c in candidates
        if c["issue_dt"] is not None and c["issue_dt"].date() > c["summary_date"]
    ]
    if finalized:
        return max(finalized, key=lambda x: x["issue_dt"])

    return max(candidates, key=lambda x: x["issue_dt"])


def build_source_url(version: int) -> str:
    """Reconstruct the source URL for traceability/debugging."""
    return (
        f"{BASE_URL}?site={SITE}&issuedby={ISSUEDBY}"
        f"&product={PRODUCT}&format={FORMAT}&version={version}&glossary={GLOSSARY}"
    )


def build_s3_key(summary_date: str) -> str:
    """Build S3 key for a given summary date."""
    filename = f"{summary_date}_ksfo_cli.json"
    return f"{S3_PREFIX}/{filename}"


def s3_key_exists(s3_client, bucket: str, key: str) -> bool:
    """Check if an S3 object already exists to avoid overwriting."""
    resp = s3_client.list_objects_v2(Bucket=bucket, Prefix=key, MaxKeys=1)
    return "Contents" in resp


def parse_value_token(token: str) -> dict[str, Any]:
    """
    Parse numeric tokens with support for:
      - MM (missing)
      - T (trace)
      - R (record flag)
    """
    token = token.strip().upper()

    if token == "MM":
        return {"value": None, "code": "MM"}

    if token == "T":
        return {"value": None, "code": "T"}

    m = re.fullmatch(r"(-?\d+(?:\.\d+)?)(R)?", token)
    if m:
        num_str, record_flag = m.groups()
        value = float(num_str) if "." in num_str else int(num_str)

        return {
            "value": value,
            "code": "R" if record_flag else None,
        }

    return {"value": None, "code": token}


# =========================================================
# SECTION PARSERS
# =========================================================
def extract_section(report_text: str, start_header: str, next_headers: list[str]) -> str:
    """
    Extract a section of the CLI report between headers.

    This allows each parser to operate on a clean subsection of the report.
    """
    start_pat = re.escape(start_header)

    if next_headers:
        next_pat = "|".join(re.escape(h) for h in next_headers)
        m = re.search(
            rf"{start_pat}\n(.*?)(?=\n(?:{next_pat})\n|\n\$\$)",
            report_text,
            flags=re.DOTALL,
        )
    else:
        m = re.search(
            rf"{start_pat}\n(.*)",
            report_text,
            flags=re.DOTALL,
        )

    if not m:
        return ""

    return m.group(1).strip()


# (Remaining section parsers follow same idea—each isolates and parses a block)

def parse_cli_report(report_text: str, version: int) -> Optional[dict[str, Any]]:
    """
    Convert raw CLI text into structured JSON.

    This is the main transformation step combining all section parsers.
    """
    issue_dt = parse_issue_timestamp(report_text)
    summary_date = parse_summary_date(report_text)

    if not issue_dt or not summary_date:
        return None

    issue_dt_utc = issue_dt.in_timezone("UTC")

    report = {
        "station": "KSFO",
        "site": SITE,
        "issuedby": ISSUEDBY,
        "product": PRODUCT,
        "summary_date": str(summary_date),
        "issue_timestamp_local": issue_dt.to_iso8601_string(),
        "issue_timestamp_utc": issue_dt_utc.to_iso8601_string(),
        "version_at_scrape_time": version,
        "source_url": build_source_url(version),
        "is_5pm_preliminary": is_5pm_preliminary(report_text),
        "is_finalized_candidate": issue_dt.date() > summary_date,
        # Section parsers populate structured weather data
        "temperature": parse_temperature(report_text),
        "precipitation": parse_precipitation(report_text),
        "snowfall": parse_snowfall(report_text),
        "degree_days": parse_degree_days(report_text),
        "wind": parse_wind(report_text),
        "sky_cover": parse_sky_cover(report_text),
        "weather_conditions": parse_weather_conditions(report_text),
        "relative_humidity": parse_relative_humidity(report_text),
        "normals": parse_normals(report_text),
        "sunrise_sunset": parse_sunrise_sunset(report_text),
    }

    return {
        "summary_date": summary_date,
        "issue_dt": issue_dt,
        "issue_dt_utc": issue_dt_utc,
        "version": version,
        "parsed_json": report,
    }


# =========================================================
# DAG
# =========================================================
@dag(
    dag_id="nws_cli_ksfo_to_s3_json",
    start_date=pendulum.datetime(2026, 3, 1, tz=TZ),
    schedule="15 2 * * *",
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    params={"mode": "daily"},  # "daily" or "backfill"
    tags=["weather", "nws", "cli", "ksfo", "kalshi", "s3"],
)
def nws_cli_ksfo_to_s3_json():
    """
    DAG that scrapes NWS CLI reports, selects canonical versions,
    and stores them as structured JSON in S3.
    """

    @task()
    def scrape_visible_versions() -> list[dict[str, Any]]:
        """
        Scan recent NWS report versions and parse all valid candidates.
        """
        session = requests.Session()
        results: list[dict[str, Any]] = []

        for version in range(1, MAX_VERSIONS_TO_SCAN + 1):
            try:
                html = fetch_version_html(session, version)
                report_text = extract_pre_block_text(html)

                parsed = parse_cli_report(report_text, version)
                if not parsed:
                    logging.info("Version %s skipped: unable to parse", version)
                    continue

                results.append(parsed)

            except Exception as exc:
                logging.warning("Failed version=%s: %s", version, exc)

        if not results:
            raise AirflowException("No CLI reports could be parsed.")

        return results

    @task()
    def select_canonical_reports(scraped: list[dict[str, Any]], **context) -> list[dict[str, Any]]:
        """
        Choose one canonical report per summary_date.

        Modes:
          - daily: only yesterday
          - backfill: all available dates
        """
        mode = context["params"].get("mode", "daily")
        local_now = pendulum.now(TZ)
        target_summary_date = local_now.subtract(days=1).date()

        grouped: dict[pendulum.Date, list[dict[str, Any]]] = defaultdict(list)
        for item in scraped:
            grouped[item["summary_date"]].append(item)

        selected: list[dict[str, Any]] = []

        if mode == "daily":
            candidates = grouped.get(target_summary_date, [])
            if not candidates:
                raise AirflowException("No reports found for target date")
            selected.append(choose_best_candidate(candidates))

        elif mode == "backfill":
            for summary_date, candidates in grouped.items():
                try:
                    selected.append(choose_best_candidate(candidates))
                except Exception as exc:
                    logging.warning("Skipping %s: %s", summary_date, exc)

        else:
            raise AirflowException(f"Unsupported mode: {mode}")

        return sorted(selected, key=lambda x: x["summary_date"])

    @task()
    def upload_new_reports(selected: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """
        Upload new reports to S3, skipping ones that already exist.
        """
        s3 = boto3.client("s3")
        uploaded: list[dict[str, Any]] = []

        for item in selected:
            key = build_s3_key(str(item["summary_date"]))

            if s3_key_exists(s3, S3_BUCKET, key):
                logging.info("Skipping existing key: s3://%s/%s", S3_BUCKET, key)
                continue

            s3.put_object(
                Bucket=S3_BUCKET,
                Key=key,
                Body=json.dumps(item["parsed_json"], indent=2).encode("utf-8"),
                ContentType="application/json",
            )

            uploaded.append({
                "summary_date": str(item["summary_date"]),
                "issue_timestamp_utc": item["parsed_json"]["issue_timestamp_utc"],
                "s3_uri": f"s3://{S3_BUCKET}/{key}",
            })

        return uploaded

    scraped = scrape_visible_versions()
    selected = select_canonical_reports(scraped)
    upload_new_reports(selected)


dag = nws_cli_ksfo_to_s3_json()