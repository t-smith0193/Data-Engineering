from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

import boto3
import pendulum

# S3 bucket and prefix where intraday KSFO 5-minute snapshot files are stored.
BUCKET = "weather-kalshi-data"
BASE_PREFIX = "raw/madis/KSFO/5-min"

# Local timezone used to determine which calendar day should be finalized.
TIMEZONE = "America/Los_Angeles"

# Reuse a single S3 client for this DAG file.
s3 = boto3.client("s3")


def finalize_previous_day(**context):
    """
    Replace the previous day's rolling snapshot files with a single final.csv file.

    Workflow:
      1. Look at yesterday's partition in S3.
      2. Find the latest snapshot CSV for that day.
      3. Copy it to final.csv.
      4. Delete the older snapshot files.

    This keeps the final daily output easy to consume while still allowing
    snapshot-style writes during the day.
    """
    now = pendulum.now(TIMEZONE)
    target_date = now.subtract(days=1).format("YYYY-MM-DD")
    prefix = f"{BASE_PREFIX}/{target_date}/"

    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=BUCKET, Prefix=prefix)

    keys = []
    for page in pages:
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".csv"):
                keys.append(key)

    if not keys:
        print(f"No files found for {target_date}")
        return

    # Ignore an existing final.csv so reruns do not accidentally treat it
    # as the newest intraday snapshot.
    snapshot_keys = [k for k in keys if not k.endswith("/final.csv")]
    if not snapshot_keys:
        print(f"No snapshot files found for {target_date}")
        return

    # Assuming snapshot filenames sort chronologically, the last one is the
    # most recent version of the day's data.
    latest_key = sorted(snapshot_keys)[-1]
    final_key = f"{prefix}final.csv"

    print(f"Latest snapshot: {latest_key}")
    print(f"Writing final file: {final_key}")

    # Copy the newest snapshot into a stable final filename for downstream use.
    s3.copy_object(
        Bucket=BUCKET,
        CopySource={"Bucket": BUCKET, "Key": latest_key},
        Key=final_key,
        ContentType="text/csv",
        MetadataDirective="REPLACE",
    )

    delete_keys = [{"Key": k} for k in snapshot_keys]

    # S3 delete_objects supports up to 1000 keys per request, so batch deletes
    # to handle large numbers of snapshot files safely.
    for i in range(0, len(delete_keys), 1000):
        chunk = delete_keys[i:i + 1000]
        s3.delete_objects(Bucket=BUCKET, Delete={"Objects": chunk})

    print(f"Deleted {len(snapshot_keys)} snapshot files for {target_date}")
    print(f"Left final file: s3://{BUCKET}/{final_key}")


with DAG(
    dag_id="madis_ksfo_5min_finalize",
    schedule="30 0 * * *",
    start_date=pendulum.now(TIMEZONE).subtract(days=1),
    catchup=False,
    tags=["weather", "madis", "KSFO", "cleanup"],
) as dag:
    # Single task DAG: finalize yesterday's folder after the day has ended.
    finalize_task = PythonOperator(
        task_id="finalize_previous_day",
        python_callable=finalize_previous_day,
    )