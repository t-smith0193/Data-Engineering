from __future__ import annotations

from datetime import datetime, timedelta
import os
import subprocess

from airflow import DAG
from airflow.operators.python import PythonOperator


# Absolute path to the script that builds the current-day lookup table.
SCRIPT_PATH = "/opt/airflow/src/build_current_day_market_weather_lookup.py"


def run_current_day_market_weather_lookup() -> None:
    """
    Execute the external Python script responsible for building the
    current-day Kalshi market ↔ weather lookup dataset.

    This function acts as a thin wrapper so the script can be orchestrated
    via Airflow while preserving its standalone execution behavior.
    """
    # Fail fast if the script is missing (helps catch deployment issues).
    if not os.path.exists(SCRIPT_PATH):
        raise FileNotFoundError(f"Script not found: {SCRIPT_PATH}")

    # Run the script as a subprocess so it executes in isolation from Airflow.
    result = subprocess.run(
        ["python", SCRIPT_PATH],
        capture_output=True,  # Capture stdout/stderr for logging
        text=True,            # Return output as strings instead of bytes
        check=False,          # We'll handle non-zero exit codes manually
    )

    # Always print stdout for observability in Airflow logs.
    print("STDOUT:")
    print(result.stdout)

    # Print stderr only if present (useful for debugging failures).
    if result.stderr:
        print("STDERR:")
        print(result.stderr)

    # Treat any non-zero exit code as a task failure.
    if result.returncode != 0:
        raise RuntimeError(f"Script failed with return code {result.returncode}")


# Standard Airflow task defaults.
default_args = {
    "owner": "tyler",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="current_day_market_weather_lookup",
    default_args=default_args,
    description="Build current day Kalshi market weather lookup parquet",
    start_date=datetime(2026, 3, 25),
    schedule="0 */6 * * *",  # Run every 6 hours to keep lookup fresh intraday
    catchup=False,
    max_active_runs=1,
    tags=["weather", "kalshi", "duckdb", "parquet"],
) as dag:

    # Single-task DAG that delegates all heavy lifting to the external script.
    build_lookup = PythonOperator(
        task_id="build_current_day_market_weather_lookup",
        python_callable=run_current_day_market_weather_lookup,
    )