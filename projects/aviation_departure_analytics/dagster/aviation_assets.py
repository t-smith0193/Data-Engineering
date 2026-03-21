from dagster import asset, define_asset_job, ScheduleDefinition
import requests
import json
from pathlib import Path
from datetime import datetime
import os


@asset
def raw_cle_departures():
    api_key = os.getenv("AVIATIONSTACK_API_KEY")

    url = "https://api.aviationstack.com/v1/flights"
    params = {
        "access_key": api_key,
        "dep_iata": "CLE",
        "limit": 100,
    }

    response = requests.get(url, params=params)
    response.raise_for_status()
    data = response.json()

    project_root = Path(__file__).resolve().parents[4]
    out_dir = project_root / "data" / "raw"
    out_dir.mkdir(parents=True, exist_ok=True)

    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    file_path = out_dir / f"cle_departures_{ts}.json"

    with open(file_path, "w") as f:
        json.dump(data, f, indent=2)

    return str(file_path)


# Job
daily_job = define_asset_job(
    name="daily_cle_departures_job",
    selection=["raw_cle_departures"],
)

# Schedule (6 AM daily)
daily_schedule = ScheduleDefinition(
    job=daily_job,
    cron_schedule="0 6 * * *",
)