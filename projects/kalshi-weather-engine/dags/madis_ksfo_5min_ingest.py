from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

import io
import gzip
import re
from datetime import timedelta
from urllib.parse import urljoin

import boto3
from botocore.exceptions import ClientError
import numpy as np
import pandas as pd
import pendulum
import requests
import xarray as xr

# =========================================================
# CONFIG
# =========================================================
# S3 destination for intraday MADIS snapshots.
BUCKET = "weather-kalshi-data"
BASE_PREFIX = "raw/madis/KSFO/5-min"

# Local timezone for partitioning and formatting timestamps.
TIMEZONE = "America/Los_Angeles"

# Public MADIS directory containing compressed NetCDF files.
MADIS_INDEX_URL = "https://madis-data.ncep.noaa.gov/madisPublic1/data/LDAD/hfmetar/netCDF/"

s3 = boto3.client("s3")

# Regex to extract filenames like YYYYMMDD_HHMM.gz from HTML index.
FILE_RE = re.compile(r'href="(\d{8}_\d{4}\.gz)"')


# =========================================================
# HELPERS
# =========================================================
def round_half_up(series: pd.Series) -> pd.Series:
    """
    Round values using "round half up" instead of numpy/pandas default.
    Ensures .5 values always round upward (important for temperature reporting).
    """
    return np.floor(series + 0.5)


def decode_arr(arr):
    """
    Decode byte arrays from NetCDF into clean string values.
    Handles both byte and non-byte types safely.
    """
    out = []
    for x in arr:
        if isinstance(x, (bytes, np.bytes_)):
            out.append(x.decode("utf-8", errors="ignore").strip())
        else:
            out.append(str(x).strip())
    return out


def decode_temperature_c(temp_var: xr.DataArray) -> pd.Series:
    """
    Convert temperature values from NetCDF into Celsius.

    Handles:
      - scale_factor / add_offset encoding
      - units (Kelvin, Celsius, or ambiguous cases)
      - fallback heuristics when metadata is inconsistent
    """
    vals = pd.to_numeric(temp_var.values, errors="coerce")
    attrs = dict(temp_var.attrs)

    scale_factor = attrs.get("scale_factor")
    add_offset = attrs.get("add_offset", 0)

    if scale_factor is not None:
        try:
            vals = vals * float(scale_factor) + float(add_offset)
        except Exception:
            pass

    units = str(attrs.get("units", "")).strip().lower()
    med = pd.Series(vals).dropna().abs().median()

    # Normalize to Celsius based on metadata or heuristic thresholds.
    if units in {"k", "kelvin"} or "kelvin" in units:
        vals = vals - 273.15
    elif (
        units in {"c", "degc", "celsius", "degree_celsius", "degrees_celsius"}
        or "celsius" in units
    ):
        pass
    else:
        # Fallback when units are unclear.
        if med > 200:
            vals = vals - 273.15
        elif med > 80:
            vals = vals / 10.0

    return pd.Series(vals)


def list_madis_filenames():
    """
    Scrape available MADIS filenames from the public directory listing.
    """
    resp = requests.get(MADIS_INDEX_URL, timeout=30)
    resp.raise_for_status()
    return sorted(set(FILE_RE.findall(resp.text)))


def fetch_latest_available_madis_files(n=2):
    """
    Fetch the most recent available MADIS files.

    Looks at the newest ~12 files and returns the latest N that are accessible.
    """
    session = requests.Session()
    files = list_madis_filenames()

    results = []
    for fname in reversed(files[-12:]):
        url = urljoin(MADIS_INDEX_URL, fname)
        resp = session.get(url, timeout=120)
        if resp.status_code == 200:
            results.append((fname, resp.content))
            if len(results) >= n:
                break

    if not results:
        raise RuntimeError("No MADIS files available")

    return results


def build_rows_from_madis_bytes(gz_bytes):
    """
    Convert a compressed MADIS NetCDF file into a clean DataFrame.

    Steps:
      1. Decompress gzip
      2. Load NetCDF via xarray
      3. Extract station + temperature
      4. Filter for KSFO
      5. Convert to local time + Fahrenheit
    """
    with gzip.GzipFile(fileobj=io.BytesIO(gz_bytes)) as gz:
        raw_bytes = gz.read()

    ds = xr.open_dataset(io.BytesIO(raw_bytes), decode_cf=True)

    station_ids = decode_arr(ds["stationId"].values)
    temperature_c = decode_temperature_c(ds["temperature"])

    df = pd.DataFrame({
        "stationId": station_ids,
        "observationTime": pd.to_datetime(ds["observationTime"].values, unit="s", utc=True),
        "temperature_c": temperature_c,
    })

    # Only keep KSFO observations.
    df = df[df["stationId"].str.upper().isin(["KSFO", "SFO"])].copy()

    if df.empty:
        return pd.DataFrame(columns=["valid", "temp_f"])

    # Convert timestamps to local timezone and format for downstream systems.
    df["observationTime_PT"] = df["observationTime"].dt.tz_convert("America/Los_Angeles")
    df["valid"] = df["observationTime_PT"].dt.strftime("%-m/%-d/%Y %H:%M")

    # Convert Celsius → Fahrenheit with proper rounding.
    df["temp_f"] = round_half_up((df["temperature_c"] * 9 / 5) + 32).astype("Int64")

    return df[["valid", "temp_f"]]


def list_existing_day_csv_keys(bucket, prefix):
    """
    List all CSV snapshots already written for a given day.
    """
    paginator = s3.get_paginator("list_objects_v2")
    keys = []

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".csv"):
                keys.append(obj["Key"])

    return sorted(keys)


def read_existing_day_df(bucket, prefix, current_key=None):
    """
    Load the latest existing snapshot for the day.

    This enables incremental updates instead of rebuilding from scratch.
    """
    keys = list_existing_day_csv_keys(bucket, prefix)

    if current_key:
        # Avoid reading the file we're about to write.
        keys = [k for k in keys if k != current_key]

    if not keys:
        return pd.DataFrame(columns=["valid", "temp_f"])

    latest_key = keys[-1]

    resp = s3.get_object(Bucket=bucket, Key=latest_key)
    text = resp["Body"].read().decode("utf-8")

    df = pd.read_csv(io.StringIO(text))
    df["temp_f"] = pd.to_numeric(df["temp_f"], errors="coerce").astype("Int64")

    return df[["valid", "temp_f"]]


def merge_and_dedupe(existing_df, new_df):
    """
    Merge existing and new data, ensuring:
      - No duplicate timestamps
      - Most recent value is kept
      - Output is time-ordered
    """
    merged = pd.concat([existing_df, new_df], ignore_index=True)

    merged["valid"] = merged["valid"].astype(str).str.strip()
    merged["temp_f"] = pd.to_numeric(merged["temp_f"], errors="coerce").astype("Int64")

    # Deduplicate on timestamp, keeping latest value.
    merged = merged.drop_duplicates(subset=["valid"], keep="last")

    # Sort chronologically.
    merged["_dt"] = pd.to_datetime(merged["valid"], format="%m/%d/%Y %H:%M", errors="coerce")
    merged = merged.sort_values("_dt").drop(columns="_dt")

    return merged[["valid", "temp_f"]]


def fetch_transform_and_store(**context):
    """
    End-to-end pipeline:
      1. Fetch latest MADIS files
      2. Transform into structured rows
      3. Merge with existing day data
      4. Write new snapshot to S3
    """
    now = pendulum.now(TIMEZONE)
    date_str = now.format("YYYY-MM-DD")
    ts_str = now.format("YYYY-MM-DDTHH-mm-ss")

    madis_files = fetch_latest_available_madis_files(2)

    new_frames = []
    for fname, gz_bytes in madis_files:
        df = build_rows_from_madis_bytes(gz_bytes)
        new_frames.append(df)

    # Combine and dedupe across multiple source files.
    combined_new = pd.concat(new_frames, ignore_index=True)
    combined_new = merge_and_dedupe(pd.DataFrame(columns=["valid", "temp_f"]), combined_new)

    current_key = f"{BASE_PREFIX}/{date_str}/{ts_str}.csv"
    day_prefix = f"{BASE_PREFIX}/{date_str}/"

    # Load existing snapshot and merge.
    existing_df = read_existing_day_df(BUCKET, day_prefix, current_key)
    final_df = merge_and_dedupe(existing_df, combined_new)

    # Write snapshot as CSV.
    csv_buffer = io.StringIO()
    final_df.to_csv(csv_buffer, index=False)

    s3.put_object(
        Bucket=BUCKET,
        Key=current_key,
        Body=csv_buffer.getvalue().encode("utf-8"),
        ContentType="text/csv",
    )

    print(f"Wrote s3://{BUCKET}/{current_key}")
    print(final_df.tail(20).to_string(index=False))


# =========================================================
# DAG
# =========================================================
default_args = {
    "owner": "airflow",
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="madis_ksfo_5min_ingest",
    default_args=default_args,
    schedule="*/5 * * * *",  # Run every 5 minutes for near-real-time ingestion
    start_date=pendulum.now(TIMEZONE).subtract(days=1),
    catchup=False,
    tags=["madis", "weather", "KSFO"],
) as dag:

    # Single task: fetch → transform → merge → store
    fetch_task = PythonOperator(
        task_id="fetch_transform_and_store_madis_ksfo",
        python_callable=fetch_transform_and_store,
    )

    fetch_task