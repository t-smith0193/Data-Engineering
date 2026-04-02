# Source: :contentReference[oaicite:0]{index=0}

import re
import urllib.request
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import boto3

# S3 destination for raw AFD text files.
S3_BUCKET = "weather-kalshi-data"
S3_PREFIX = "raw/nws/af_discussion/KSFO"

# Special key used to track the most recently processed product.
LATEST_KEY = f"{S3_PREFIX}/latest.txt"

# Source URL for the latest Area Forecast Discussion (AFD).
SOURCE_URL = (
    "https://forecast.weather.gov/product.php"
    "?site=MTR&issuedby=MTR&product=AFD&format=txt&version=1&glossary=0"
)

REQUEST_TIMEOUT_SECONDS = 20
USER_AGENT = "weather-kalshi-afd-ingestor/1.0"

# Reuse S3 client across Lambda invocations.
s3 = boto3.client("s3")


def fetch_latest_product_html() -> str:
    """
    Fetch the raw HTML page containing the latest AFD product.
    """
    req = urllib.request.Request(
        SOURCE_URL,
        headers={"User-Agent": USER_AGENT},
    )
    with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT_SECONDS) as response:
        return response.read().decode("utf-8", errors="replace")


def extract_raw_text(html: str) -> str:
    """
    Extract the AFD text block from the NWS HTML page.

    Steps:
      1. Strip HTML tags and scripts/styles
      2. Normalize whitespace/newlines
      3. Identify start of product (FXUS... header)
      4. Stop before footer/signature section
      5. Clean up excessive blank lines

    This ensures we store only the canonical product text.
    """
    # Normalize newlines first.
    text = html.replace("\r\n", "\n").replace("\r", "\n")

    # Remove HTML structure.
    text = re.sub(r"(?is)<script.*?>.*?</script>", "", text)
    text = re.sub(r"(?is)<style.*?>.*?</style>", "", text)
    text = re.sub(r"(?is)<br\s*/?>", "\n", text)
    text = re.sub(r"(?is)</p\s*>", "\n", text)
    text = re.sub(r"(?is)<.*?>", "", text)

    # Basic HTML entity cleanup.
    text = (
        text.replace("&nbsp;", " ")
            .replace("&amp;", "&")
            .replace("&lt;", "<")
            .replace("&gt;", ">")
    )

    lines = [line.rstrip() for line in text.split("\n")]

    # Locate start of AFD product using standard NWS header pattern.
    start_idx = None
    for i, line in enumerate(lines):
        if re.match(r"^\s*FXUS\d{2}\s+K[A-Z]{3}\s+\d{6}\s*$", line):
            start_idx = i
            break

    if start_idx is None:
        raise ValueError("Could not find start of AFD product text.")

    # Identify end of product before footer.
    end_idx = len(lines)
    for i in range(start_idx, len(lines)):
        line = lines[i].strip()
        if line.startswith("Visit us at www.weather.gov/"):
            end_idx = i
            break

    product_lines = lines[start_idx:end_idx]

    # Collapse excessive blank lines.
    cleaned = []
    blank_count = 0
    for line in product_lines:
        if line.strip() == "":
            blank_count += 1
            if blank_count <= 2:
                cleaned.append("")
        else:
            blank_count = 0
            cleaned.append(line)

    raw_text = "\n".join(cleaned).strip() + "\n"

    # Basic sanity check to confirm we extracted the correct product.
    if "AFDMTR" not in raw_text:
        raise ValueError("Extracted text does not look like the expected AFD product.")

    return raw_text


def extract_issued_at_utc(raw_text: str) -> datetime:
    """
    Parse issuance timestamp from AFD text and convert to UTC.

    Handles:
      - Variable-length time formats (e.g. 420 PM vs 0420 PM)
      - Multiple US timezone abbreviations
    """
    pattern = re.compile(
        r"(?im)^\s*(\d{1,4})\s+([AP]M)\s+"
        r"([A-Z]{2,4})\s+([A-Za-z]{3})\s+([A-Za-z]{3})\s+(\d{1,2})\s+(\d{4})\s*$"
    )

    match = pattern.search(raw_text)
    if not match:
        raise ValueError("Could not parse issuance timestamp from product text.")

    hhmm_str, ampm, tz_abbr, _, month_abbr, day_str, year_str = match.groups()

    hhmm = hhmm_str.zfill(4)
    hour = int(hhmm[:-2])
    minute = int(hhmm[-2:])

    # Convert 12-hour clock → 24-hour.
    if ampm == "AM":
        hour = 0 if hour == 12 else hour
    else:
        hour = 12 if hour == 12 else hour + 12

    month_map = {
        "JAN": 1, "FEB": 2, "MAR": 3, "APR": 4,
        "MAY": 5, "JUN": 6, "JUL": 7, "AUG": 8,
        "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12,
    }

    # Map NWS timezone abbreviations to real tzinfo.
    tz_map = {
        "PST": ZoneInfo("America/Los_Angeles"),
        "PDT": ZoneInfo("America/Los_Angeles"),
        "MST": ZoneInfo("America/Denver"),
        "MDT": ZoneInfo("America/Denver"),
        "CST": ZoneInfo("America/Chicago"),
        "CDT": ZoneInfo("America/Chicago"),
        "EST": ZoneInfo("America/New_York"),
        "EDT": ZoneInfo("America/New_York"),
        "UTC": timezone.utc,
        "GMT": timezone.utc,
    }

    issued_local = datetime(
        int(year_str),
        month_map[month_abbr.upper()],
        int(day_str),
        hour,
        minute,
        tzinfo=tz_map[tz_abbr.upper()],
    )

    return issued_local.astimezone(timezone.utc)


def read_latest_timestamp():
    """
    Read the last processed issuance timestamp from S3.

    Used to make ingestion idempotent (skip duplicates).
    """
    try:
        response = s3.get_object(Bucket=S3_BUCKET, Key=LATEST_KEY)
        body = response["Body"].read().decode("utf-8").strip()
        return body or None
    except Exception as exc:
        error_code = getattr(exc, "response", {}).get("Error", {}).get("Code")
        if error_code in {"NoSuchKey", "404"}:
            return None
        raise


def write_raw_product(issued_at_utc: datetime, raw_text: str) -> str:
    """
    Store raw AFD text in S3 partitioned by date.

    Structure:
      raw/nws/af_discussion/KSFO/YYYY-MM-DD/<timestamp>.txt
    """
    day_folder = issued_at_utc.strftime("%Y-%m-%d")
    filename = issued_at_utc.strftime("%Y-%m-%dT%H-%M-%SZ.txt")
    s3_key = f"{S3_PREFIX}/{day_folder}/{filename}"

    s3.put_object(
        Bucket=S3_BUCKET,
        Key=s3_key,
        Body=raw_text.encode("utf-8"),
        ContentType="text/plain; charset=utf-8",
    )
    return s3_key


def write_latest_timestamp(issued_at_utc_str: str) -> None:
    """
    Update the "latest" marker in S3 to track most recent processed product.
    """
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=LATEST_KEY,
        Body=(issued_at_utc_str + "\n").encode("utf-8"),
        ContentType="text/plain; charset=utf-8",
    )


def lambda_handler(event, context):
    """
    Main Lambda entrypoint.

    Workflow:
      1. Fetch latest AFD HTML
      2. Extract clean product text
      3. Parse issuance timestamp
      4. Compare against last processed timestamp
      5. Store only if new (idempotent ingestion)
    """
    html = fetch_latest_product_html()
    raw_text = extract_raw_text(html)
    issued_at_utc = extract_issued_at_utc(raw_text)
    issued_at_utc_str = issued_at_utc.strftime("%Y-%m-%dT%H:%M:%SZ")

    latest_seen = read_latest_timestamp()

    # Skip if we've already processed this version.
    if latest_seen == issued_at_utc_str:
        return {
            "status": "unchanged",
            "issued_at_utc": issued_at_utc_str,
            "latest_key": LATEST_KEY,
        }

    # Store new product and update latest pointer.
    stored_key = write_raw_product(issued_at_utc, raw_text)
    write_latest_timestamp(issued_at_utc_str)

    return {
        "status": "stored",
        "issued_at_utc": issued_at_utc_str,
        "s3_key": stored_key,
        "latest_key": LATEST_KEY,
    }