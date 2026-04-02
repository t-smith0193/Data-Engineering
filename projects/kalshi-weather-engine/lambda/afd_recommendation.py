import os
import re
import json
import urllib.parse
import urllib.request
from decimal import Decimal
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import boto3
from openai import OpenAI

# Reuse S3 client across Lambda invocations.
s3 = boto3.client("s3")

# Environment configuration.
OPENAI_API_KEY = os.environ["OPENAI_API_KEY"]
SLACK_WEBHOOK_URL = os.environ["SLACK_WEBHOOK_URL"]
OPENAI_MODEL = os.environ.get("OPENAI_MODEL", "gpt-4.1-mini")

# Timezones used for weather interpretation and message display.
PT_TZ = ZoneInfo("America/Los_Angeles")
ET_TZ = ZoneInfo("America/New_York")

# S3 locations for market metadata and model outputs.
SOURCE_BUCKET = "weather-kalshi-data"
MARKET_PREFIX = "raw/kalshi/historical/market/KXHIGHTSFO"
OUTPUT_PREFIX = "processed/afd_recommendation/KSFO"

# OpenAI client used to generate structured weather-driven market forecasts.
client = OpenAI(api_key=OPENAI_API_KEY)


def post_to_slack(message: str) -> None:
    """
    Send a formatted message to Slack via webhook.
    """
    payload = json.dumps({"text": message}).encode("utf-8")
    req = urllib.request.Request(
        SLACK_WEBHOOK_URL,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=20) as resp:
        if resp.status >= 300:
            raise RuntimeError(f"Slack webhook failed with status {resp.status}")


def load_s3_text(bucket: str, key: str) -> str:
    """
    Read a text object from S3.
    """
    obj = s3.get_object(Bucket=bucket, Key=key)
    return obj["Body"].read().decode("utf-8")


def load_s3_json(bucket: str, key: str) -> dict:
    """
    Read and parse a JSON object from S3.
    """
    return json.loads(load_s3_text(bucket, key))


def s3_key_exists(bucket: str, key: str) -> bool:
    """
    Return True if an S3 key exists, otherwise False.

    This is used to safely probe candidate market files without failing
    when some dates are not yet available.
    """
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except Exception as exc:
        code = getattr(exc, "response", {}).get("Error", {}).get("Code")
        if code in {"404", "NoSuchKey", "NotFound"}:
            return False
        raise


def extract_s3_event_record(event: dict) -> tuple[str, str]:
    """
    Extract the source bucket and key from the triggering S3 event.
    """
    record = event["Records"][0]
    bucket = record["s3"]["bucket"]["name"]
    key = urllib.parse.unquote_plus(record["s3"]["object"]["key"])
    return bucket, key


def decimal_to_cents(value: str | None) -> int | None:
    """
    Convert a dollar-denominated price string into integer cents.

    Example:
      "0.47" -> 47
    """
    if value is None or value == "":
        return None
    return int((Decimal(value) * 100).quantize(Decimal("1")))


def midpoint_cents(bid: str | None, ask: str | None, last: str | None) -> int | None:
    """
    Estimate a fair reference price using bid/ask midpoint when available.

    If both bid and ask exist, use their midpoint.
    Otherwise, fall back to the last traded price.
    """
    bid_c = decimal_to_cents(bid)
    ask_c = decimal_to_cents(ask)
    last_c = decimal_to_cents(last)

    if bid_c is not None and ask_c is not None:
        return int(round((bid_c + ask_c) / 2))
    return last_c


def extract_issued_at_utc(raw_text: str) -> datetime:
    """
    Parse the AFD issuance timestamp from raw forecast text and convert to UTC.

    This supports the standard NWS timestamp line format and multiple timezone
    abbreviations (PDT, PST, EDT, etc.).
    """
    pattern = re.compile(
        r"(?im)^\s*(\d{1,4})\s+([AP]M)\s+"
        r"([A-Z]{2,4})\s+([A-Za-z]{3})\s+([A-Za-z]{3})\s+(\d{1,2})\s+(\d{4})\s*$"
    )
    match = pattern.search(raw_text)
    if not match:
        raise ValueError("Could not parse issuance timestamp from AFD text.")

    hhmm_str, ampm, tz_abbr, _, month_abbr, day_str, year_str = match.groups()
    tz_abbr = tz_abbr.upper()
    month_abbr = month_abbr.upper()

    month_map = {
        "JAN": 1, "FEB": 2, "MAR": 3, "APR": 4,
        "MAY": 5, "JUN": 6, "JUL": 7, "AUG": 8,
        "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12,
    }
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

    hhmm = hhmm_str.zfill(4)
    hour = int(hhmm[:-2])
    minute = int(hhmm[-2:])

    # Convert 12-hour clock to 24-hour clock.
    if ampm.upper() == "AM":
        hour = 0 if hour == 12 else hour
    else:
        hour = 12 if hour == 12 else hour + 12

    issued_local = datetime(
        int(year_str),
        month_map[month_abbr],
        int(day_str),
        hour,
        minute,
        tzinfo=tz_map[tz_abbr],
    )
    return issued_local.astimezone(timezone.utc)


def extract_section(raw_text: str, section_name: str) -> str:
    """
    Extract a named section from the AFD text.

    Sections typically begin with a dot-prefixed header like:
      .SHORT TERM...
      .AVIATION...

    Parsing stops when the next section header begins.
    """
    lines = raw_text.splitlines()
    target = section_name.upper()

    start_idx = None
    for i, line in enumerate(lines):
        normalized = line.strip().upper()
        if normalized.startswith(f".{target}") or normalized == target:
            start_idx = i
            break

    if start_idx is None:
        return ""

    section_lines = [lines[start_idx]]
    for line in lines[start_idx + 1:]:
        stripped = line.strip()
        upper = stripped.upper()

        # Stop when a new AFD section begins.
        if stripped.startswith(".") and re.match(r"^\.[A-Z /]+", upper):
            break

        section_lines.append(line)

    return "\n".join(section_lines).strip()


def filter_short_term_for_ksfo(short_term_text: str) -> str:
    """
    Keep only KSFO/coastal-relevant lines from the SHORT TERM section.

    This reduces noise before sending the forecast text to the model.
    If no keyword matches are found, fall back to the full section.
    """
    if not short_term_text:
        return ""

    lines = short_term_text.splitlines()
    kept = []

    keywords = [
        "SAN FRANCISCO",
        "SF",
        "BAY AREA",
        "COAST",
        "COASTAL",
        "PENINSULA",
        "MARINE LAYER",
        "OFFSHORE",
        "ONSHORE",
        "FOG",
        "STRATUS",
        "COASTAL HIGHS",
        "TEMPERATURE",
        "WARM",
        "COOL",
    ]

    for line in lines:
        upper = line.upper()
        if any(keyword in upper for keyword in keywords):
            kept.append(line)

    return "\n".join(kept).strip() if kept else short_term_text.strip()


def extract_sfo_relevant_text(aviation_text: str) -> str:
    """
    Extract the most SFO-relevant portions of the AVIATION section.

    The goal is to isolate KSFO-specific aviation discussion, especially
    references to SFO, KSFO, bridge approach, and nearby marine conditions.
    """
    if not aviation_text:
        return ""

    lines = aviation_text.splitlines()
    kept = []
    current_block_lines = []
    current_header = None

    def flush_block():
        nonlocal current_block_lines, current_header
        if not current_block_lines:
            return

        block_text = "\n".join(current_block_lines).strip()
        header_upper = (current_header or "").upper()
        block_upper = block_text.upper()

        keep = False
        if "SFO" in header_upper or "KSFO" in header_upper:
            keep = True
        if "SFO BRIDGE APPROACH" in block_upper:
            keep = True
        if "KSFO" in block_upper:
            keep = True
        if "VICINITY OF SFO" in block_upper:
            keep = True

        if keep:
            kept.append(block_text)

        current_block_lines = []
        current_header = None

    block_header_pattern = re.compile(r"^[A-Z0-9 /.-]*SFO[A-Z0-9 /.-]*\.\.\.$", re.I)

    for line in lines:
        stripped = line.strip()

        if not stripped:
            if current_block_lines:
                current_block_lines.append("")
            continue

        # Start a new block when we hit an SFO-specific subheader.
        if block_header_pattern.match(stripped) or stripped.upper().startswith("VICINITY OF SFO"):
            flush_block()
            current_header = stripped
            current_block_lines = [stripped]
            continue

        # Start a new non-SFO block so we can stop keeping the previous one.
        if re.match(r"^[A-Z0-9 /.-]+\.\.\.$", stripped) and "SFO" not in stripped.upper():
            flush_block()
            current_header = stripped
            current_block_lines = [stripped]
            continue

        if current_block_lines:
            current_block_lines.append(line)

    flush_block()

    # Preferred path: return extracted SFO blocks.
    if kept:
        return "\n\n".join(kept).strip()

    # Fallback: keep any individual lines with SFO-related keywords.
    filtered = []
    for line in lines:
        upper = line.upper()
        if "SFO" in upper or "KSFO" in upper or "BRIDGE APPROACH" in upper:
            filtered.append(line)

    if filtered:
        return "\n".join(filtered).strip()

    return aviation_text.strip()


def candidate_market_dates(issued_local: datetime) -> list[str]:
    """
    Build candidate market dates based on the local AFD issuance date.

    We consider:
      - today
      - tomorrow
      - day after tomorrow

    This gives the model a constrained set of valid target dates to choose from.
    """
    base = issued_local.date()
    return [
        (base + timedelta(days=offset)).isoformat()
        for offset in range(0, 3)
    ]


def load_candidate_market_files(candidate_dates: list[str]) -> list[dict]:
    """
    Load Kalshi market metadata for each candidate event date.

    For each available market file, keep only the fields needed by the model:
      - contract labels / strikes
      - bid / ask / last / midpoint prices
      - event identifiers
    """
    candidates = []

    for event_date in candidate_dates:
        key = f"{MARKET_PREFIX}/{event_date}_KXHIGHTSFO.json"
        if not s3_key_exists(SOURCE_BUCKET, key):
            continue

        data = load_s3_json(SOURCE_BUCKET, key)
        markets = []

        for market in data.get("markets", []):
            markets.append(
                {
                    "ticker": market["ticker"],
                    "label": market["yes_sub_title"],
                    "strike_type": market.get("strike_type"),
                    "floor_strike": market.get("floor_strike"),
                    "cap_strike": market.get("cap_strike"),
                    "yes_bid_cents": decimal_to_cents(market.get("yes_bid_dollars")),
                    "yes_ask_cents": decimal_to_cents(market.get("yes_ask_dollars")),
                    "last_price_cents": decimal_to_cents(market.get("last_price_dollars")),
                    "mid_price_cents": midpoint_cents(
                        market.get("yes_bid_dollars"),
                        market.get("yes_ask_dollars"),
                        market.get("last_price_dollars"),
                    ),
                }
            )

        candidates.append(
            {
                "event_date": data["event_date"],
                "event_ticker": data["event_ticker"],
                "title": data["title"],
                "markets": markets,
            }
        )

    if not candidates:
        raise ValueError("No candidate KXHIGHTSFO market files found in S3.")

    return candidates


def build_reference_dates(issued_local: datetime) -> dict:
    """
    Build a helper map so the model can resolve relative time phrases.

    Example:
      - today
      - tomorrow
      - weekday names like monday / tuesday
    """
    names = {}
    for offset in range(0, 7):
        d = issued_local.date() + timedelta(days=offset)
        names[d.strftime("%A").lower()] = d.isoformat()

    return {
        "issued_local": issued_local.isoformat(),
        "today": issued_local.date().isoformat(),
        "tomorrow": (issued_local.date() + timedelta(days=1)).isoformat(),
        "day_after_tomorrow": (issued_local.date() + timedelta(days=2)).isoformat(),
        "weekday_map": names,
    }


def build_prompt(
    issued_local: datetime,
    short_term_text_filtered: str,
    sfo_aviation_text: str,
    candidate_markets_json: list[dict],
) -> str:
    """
    Build the model prompt that maps forecast reasoning to Kalshi markets.

    The prompt constrains the model to:
      - use only the provided weather text
      - choose exactly one valid event date
      - assign probabilities across all buckets
      - return strict JSON output
    """
    reference_dates_json = json.dumps(build_reference_dates(issued_local), indent=2)
    candidate_markets_str = json.dumps(candidate_markets_json, indent=2)

    return f"""
You are a weather-driven trading model for Kalshi temperature markets.

Your objective is to forecast the DAILY MAX TEMPERATURE at KSFO (San Francisco International Airport) and map that forecast to the provided market buckets.

KSFO is a coastal airport. Its temperature is strongly influenced by marine air, cloud cover, and airflow patterns.

You must:

Determine which calendar day the forecast refers to
Estimate the KSFO daily maximum temperature for that day
Assign probabilities across all provided market buckets
Explain the physical weather mechanisms driving that outcome

IMPORTANT CONSTRAINTS:

You must use ONLY the provided forecast text
You must include ALL buckets exactly once
Probabilities must sum to 100

CRITICAL MODELING RULES:

The target variable is:
→ DAILY MAX TEMPERATURE at KSFO

You MUST prioritize:

Explicit coastal/SF temperature forecasts
Marine layer presence or absence
Offshore vs onshore flow
Cloud and fog timing (especially afternoon clearing)
Coastal vs inland temperature differences

You MUST NOT rely on:

Aviation conditions alone
Wind conditions unless they affect temperature
Inland temperatures without coastal confirmation

INTERPRETATION RULES:

Offshore flow → strong warming (downslope + marine suppression)
No marine layer → strong warming
Marine layer present → cooling cap
Coastal highs in the 70s → very strong bullish signal
Inland heat WITHOUT coastal mention → weak signal for KSFO
Fog/clouds persisting into afternoon → caps highs

KSFO is often much cooler than inland areas.
Always discount inland heat unless explicitly stated to reach the coast.

DATE RESOLUTION RULES:

Resolve all time references using provided reference dates.

Examples:

"today", "this afternoon" → same day as issued_local
"tomorrow" → next calendar day
weekday names → use weekday_map
"afternoon highs" → daily max temperature

You MUST select exactly one target_event_date from the candidate markets.

PHYSICAL REASONING REQUIREMENT:

You must explain WHY the temperature outcome occurs using atmospheric processes.

Your reasoning must include:

airflow direction (offshore/onshore)
marine layer behavior
cloud/fog evolution
how these impact solar heating and temperature at KSFO

Do NOT just summarize the forecast.
Explain the cause-and-effect chain.

You must fully populate all required fields.
Partial or minimal outputs are INVALID.

Issued local time:
{issued_local.isoformat()}

Reference dates:
{reference_dates_json}

---

FORECAST DATA (FOCUS ON KSFO / COASTAL CONDITIONS):

SHORT TERM (coastal / SF relevant):
{short_term_text_filtered}

AVIATION (KSFO relevant only):
{sfo_aviation_text}

---

CANDIDATE MARKET CONTRACTS:
{candidate_markets_str}

---

Return a single JSON object with this structure:

{{
  "target_event_date": "YYYY-MM-DD",
  "estimated_max_temp_f": 77,
  "estimated_range_f": {{
    "low": 75,
    "high": 79
  }},
  "physical_reasoning": "Detailed pilot-level explanation of the weather mechanisms and why the probabilities were chosen.",
  "markets": [
    {{
      "ticker": "string",
      "probability_pct": 0,
      "yes_fair_value_cents": 0
    }}
  ]
}}

Rules for the JSON:
- target_event_date must be one of the candidate market dates
- estimated_max_temp_f must be an integer
- estimated_range_f.low and estimated_range_f.high must be integers
- markets must include every contract from the chosen date exactly once
- probability_pct values must be integers
- yes_fair_value_cents must equal probability_pct
- all probability_pct values must sum to exactly 100
""".strip()


def generate_ai_forecast(
    issued_local: datetime,
    short_term_text_filtered: str,
    sfo_aviation_text: str,
    candidates: list[dict],
) -> dict:
    """
    Call the OpenAI model and parse the returned JSON forecast.
    """
    prompt = build_prompt(
        issued_local=issued_local,
        short_term_text_filtered=short_term_text_filtered,
        sfo_aviation_text=sfo_aviation_text,
        candidate_markets_json=candidates,
    )

    response = client.responses.create(
        model=OPENAI_MODEL,
        input=prompt,
    )

    raw_output = response.output_text.strip()
    return json.loads(raw_output)


def normalize_forecast(raw_forecast: dict, candidates: list[dict]) -> tuple[dict, dict]:
    """
    Validate and normalize model output against the available market set.

    This enforces:
      - required keys
      - valid chosen date
      - exact market coverage
      - probability sum = 100
      - fair value consistency
    """
    required_keys = {
        "target_event_date",
        "estimated_max_temp_f",
        "estimated_range_f",
        "physical_reasoning",
        "markets",
    }
    missing = required_keys - set(raw_forecast.keys())
    if missing:
        raise ValueError(f"Forecast missing required keys: {sorted(missing)}")

    candidates_by_date = {c["event_date"]: c for c in candidates}
    target_date = raw_forecast["target_event_date"]

    if target_date not in candidates_by_date:
        raise ValueError(f"Model selected unavailable target_event_date: {target_date}")

    chosen = candidates_by_date[target_date]
    rows_by_ticker = {m["ticker"]: m for m in chosen["markets"]}

    normalized_markets = []
    for market in raw_forecast["markets"]:
        ticker = market["ticker"]
        if ticker not in rows_by_ticker:
            raise ValueError(f"Ticker not found in chosen candidate set: {ticker}")

        row = rows_by_ticker[ticker]
        probability = int(market["probability_pct"])
        fair_value = int(market["yes_fair_value_cents"])

        if fair_value != probability:
            raise ValueError(
                f"yes_fair_value_cents must equal probability_pct for {ticker}"
            )

        normalized_markets.append(
            {
                "ticker": ticker,
                "label": row["label"],
                "probability_pct": probability,
                "yes_fair_value_cents": fair_value,
                "no_fair_value_cents": 100 - probability,
                "strike_type": row["strike_type"],
                "floor_strike": row["floor_strike"],
                "cap_strike": row["cap_strike"],
                "market_yes_bid_cents": row["yes_bid_cents"],
                "market_yes_ask_cents": row["yes_ask_cents"],
                "market_last_price_cents": row["last_price_cents"],
                "market_mid_price_cents": row["mid_price_cents"],
            }
        )

    # Keep contracts sorted in a stable strike order for readability.
    normalized_markets.sort(
        key=lambda x: (999999 if x["floor_strike"] is None else x["floor_strike"], x["label"])
    )

    chosen_tickers = {m["ticker"] for m in normalized_markets}
    expected_tickers = {m["ticker"] for m in chosen["markets"]}

    if chosen_tickers != expected_tickers:
        missing = sorted(expected_tickers - chosen_tickers)
        extra = sorted(chosen_tickers - expected_tickers)
        raise ValueError(f"Forecast ticker mismatch. missing={missing} extra={extra}")

    total_probability = sum(m["probability_pct"] for m in normalized_markets)
    if total_probability != 100:
        raise ValueError(f"Probabilities must sum to 100, got {total_probability}")

    estimated_range = raw_forecast["estimated_range_f"]
    if "low" not in estimated_range or "high" not in estimated_range:
        raise ValueError("estimated_range_f must include low and high")

    normalized = {
        "target_event_date": target_date,
        "estimated_max_temp_f": int(raw_forecast["estimated_max_temp_f"]),
        "estimated_range_f": {
            "low": int(estimated_range["low"]),
            "high": int(estimated_range["high"]),
        },
        "physical_reasoning": str(raw_forecast["physical_reasoning"]).strip(),
        "markets": normalized_markets,
        "total_probability_pct": total_probability,
        "event_ticker": chosen["event_ticker"],
        "event_title": chosen["title"],
    }
    return normalized, chosen


def build_snapshot(
    source_bucket: str,
    source_key: str,
    issued_at_utc: datetime,
    issued_local: datetime,
    short_term_text: str,
    short_term_text_filtered: str,
    aviation_text: str,
    sfo_text: str,
    chosen_market_file_key: str,
    forecast: dict,
) -> dict:
    """
    Build the full output snapshot written to S3.

    This preserves:
      - the input forecast text used
      - the chosen market file
      - run timestamps
      - model name
      - normalized forecast output
    """
    now_utc = datetime.now(timezone.utc)
    return {
        "source_bucket": source_bucket,
        "source_key": source_key,
        "issued_at_utc": issued_at_utc.isoformat(),
        "issued_at_pt": issued_local.isoformat(),
        "short_term_text": short_term_text,
        "short_term_text_filtered": short_term_text_filtered,
        "aviation_text": aviation_text,
        "sfo_text": sfo_text,
        "chosen_market_file_key": chosen_market_file_key,
        "run_time_utc": now_utc.isoformat(),
        "run_time_pt": now_utc.astimezone(PT_TZ).isoformat(),
        "run_time_et": now_utc.astimezone(ET_TZ).isoformat(),
        "openai_model": OPENAI_MODEL,
        "forecast": forecast,
    }


def build_output_key(source_key: str, target_event_date: str) -> str:
    """
    Build an S3 output key partitioned by target market date.
    """
    source_filename = source_key.split("/")[-1].replace(".txt", "")
    return f"{OUTPUT_PREFIX}/{target_event_date}/{source_filename}.json"


def write_snapshot(snapshot: dict, key: str) -> None:
    """
    Write the forecast snapshot to S3.
    """
    s3.put_object(
        Bucket=SOURCE_BUCKET,
        Key=key,
        Body=json.dumps(snapshot, indent=2).encode("utf-8"),
        ContentType="application/json",
    )


def render_market_lines(markets: list[dict]) -> str:
    """
    Render a compact Slack-friendly summary of AI vs market pricing by bucket.
    """
    lines = []
    for m in markets:
        lines.append(
            f"{m['label']}: AI {m['yes_fair_value_cents']}¢ | Mkt {m['market_mid_price_cents']}¢"
        )
    return "\n".join(lines)


def build_slack_message(
    source_key: str,
    issued_local: datetime,
    chosen_market_file_key: str,
    forecast: dict,
) -> str:
    """
    Build the Slack message summarizing the AI forecast and bucket pricing.
    """
    temp_range = forecast["estimated_range_f"]
    return (
        f":airplane: *KSFO AFD Update*\n"
        f"AFD file: `{source_key}`\n"
        f"Issued (PT): {issued_local.strftime('%Y-%m-%d %I:%M %p %Z')}\n"
        f"Chosen market date: *{forecast['target_event_date']}*\n"
        f"Estimated KSFO max: *{forecast['estimated_max_temp_f']}F* "
        f"(range {temp_range['low']}-{temp_range['high']}F)\n"
        f"Market file: `{chosen_market_file_key}`\n\n"
        f"*Weather reasoning*\n{forecast['physical_reasoning']}\n\n"
        f"*Bucket breakdown*\n{render_market_lines(forecast['markets'])}"
    )


def lambda_handler(event, context):
    """
    Main Lambda entrypoint.

    End-to-end workflow:
      1. Trigger on new AFD text file in S3
      2. Parse issuance time and relevant forecast sections
      3. Filter text to KSFO-specific weather signals
      4. Load candidate Kalshi market files
      5. Generate AI forecast and normalize/validate it
      6. Write snapshot to S3
      7. Post summary to Slack
    """
    print(json.dumps(event, indent=2))

    source_bucket, source_key = extract_s3_event_record(event)

    # Only process KSFO AFD raw text files.
    if not source_key.startswith("raw/nws/af_discussion/KSFO/") or not source_key.endswith(".txt"):
        return {
            "statusCode": 200,
            "body": json.dumps({"ok": True, "skipped": True, "reason": "Not an AFD txt object"}),
        }

    raw_text = load_s3_text(source_bucket, source_key)
    issued_at_utc = extract_issued_at_utc(raw_text)
    issued_local = issued_at_utc.astimezone(PT_TZ)

    # Extract the main forecast sections used by the model.
    short_term_text = extract_section(raw_text, "SHORT TERM")
    aviation_text = extract_section(raw_text, "AVIATION")

    if not short_term_text:
        raise ValueError("SHORT TERM section not found in AFD text.")
    if not aviation_text:
        raise ValueError("AVIATION section not found in AFD text.")

    # Reduce noise and keep only KSFO/coastal-relevant information.
    short_term_text_filtered = filter_short_term_for_ksfo(short_term_text)
    sfo_text = extract_sfo_relevant_text(aviation_text)

    if not sfo_text:
        raise ValueError("Could not extract SFO-specific aviation text.")

    # Load the possible market dates the forecast might correspond to.
    candidate_dates = candidate_market_dates(issued_local)
    candidate_market_files = load_candidate_market_files(candidate_dates)

    # Ask the model to map forecast text to a temperature distribution.
    raw_forecast = generate_ai_forecast(
        issued_local=issued_local,
        short_term_text_filtered=short_term_text_filtered,
        sfo_aviation_text=sfo_text,
        candidates=candidate_market_files,
    )

    # Validate and normalize the model output before storing or alerting.
    forecast, chosen_candidate = normalize_forecast(raw_forecast, candidate_market_files)

    chosen_market_file_key = (
        f"{MARKET_PREFIX}/{forecast['target_event_date']}_KXHIGHTSFO.json"
    )

    snapshot = build_snapshot(
        source_bucket=source_bucket,
        source_key=source_key,
        issued_at_utc=issued_at_utc,
        issued_local=issued_local,
        short_term_text=short_term_text,
        short_term_text_filtered=short_term_text_filtered,
        aviation_text=aviation_text,
        sfo_text=sfo_text,
        chosen_market_file_key=chosen_market_file_key,
        forecast=forecast,
    )

    output_key = build_output_key(source_key, forecast["target_event_date"])
    write_snapshot(snapshot, output_key)

    slack_message = build_slack_message(
        source_key=source_key,
        issued_local=issued_local,
        chosen_market_file_key=chosen_market_file_key,
        forecast=forecast,
    )
    post_to_slack(slack_message)

    return {
        "statusCode": 200,
        "body": json.dumps(
            {
                "ok": True,
                "source_key": source_key,
                "target_event_date": forecast["target_event_date"],
                "chosen_market_file_key": chosen_market_file_key,
                "snapshot_key": output_key,
            }
        ),
    }