from __future__ import annotations

import os
from pathlib import Path

import duckdb
from dotenv import load_dotenv


def build_current_day_market_weather_lookup(output_path: str | None = None) -> str:
    """
    Build a lookup table that joins:
      - Current-day Kalshi market contracts
      - Historical weather progression models

    Output:
      A parquet file in S3 that can be used for real-time pricing / modeling.
    """
    load_dotenv()

    # Load AWS credentials from environment (.env or runtime environment).
    aws_region = (os.getenv("AWS_DEFAULT_REGION") or "").strip()
    aws_key = (os.getenv("AWS_ACCESS_KEY_ID") or "").strip()
    aws_secret = (os.getenv("AWS_SECRET_ACCESS_KEY") or "").strip()
    aws_token = (os.getenv("AWS_SESSION_TOKEN") or "").strip()

    # Fail fast if credentials are missing (prevents silent S3 failures).
    if not aws_region or not aws_key or not aws_secret:
        raise ValueError(
            "Missing AWS credentials. Check .env for AWS_DEFAULT_REGION, "
            "AWS_ACCESS_KEY_ID, and AWS_SECRET_ACCESS_KEY."
        )

    # Local project structure for DuckDB + output artifacts.
    project_root = Path(__file__).resolve().parents[1]
    data_dir = project_root / "data"
    data_dir.mkdir(parents=True, exist_ok=True)

    # Default output path (local fallback; main output is S3).
    if output_path is None:
        output_path = str(data_dir / "current_day_market_weather_lookup.parquet")

    db_path = str(data_dir / "weather_ksfo.duckdb")
    con = duckdb.connect(db_path)

    try:
        # =========================================================
        # S3 CONFIG (DuckDB httpfs extension)
        # =========================================================
        # Enables DuckDB to directly query S3 data.
        con.execute("INSTALL httpfs;")
        con.execute("LOAD httpfs;")
        con.execute(f"SET s3_region='{aws_region}';")
        con.execute(f"SET s3_access_key_id='{aws_key}';")
        con.execute(f"SET s3_secret_access_key='{aws_secret}';")
        if aws_token:
            con.execute(f"SET s3_session_token='{aws_token}';")

        # =========================================================
        # 1) RAW METAR (bootstrap + incremental)
        # =========================================================
        # Combine historical bootstrap data with incremental updates.
        con.sql("""
        CREATE OR REPLACE VIEW raw_metar_all AS
        SELECT *
        FROM read_json_auto(
            [
                's3://weather-kalshi-data/raw/awc/metar/bootstrap/KSFO/*.json',
                's3://weather-kalshi-data/raw/awc/metar/incremental/KSFO/*.json'
            ],
            union_by_name=True,
            filename=True
        )
        """)

        # =========================================================
        # 2) LOCAL TIME STAGING
        # =========================================================
        # Convert timestamps to local timezone and filter relevant fields.
        con.sql("""
        CREATE OR REPLACE VIEW staging_metar_obs_local AS
        SELECT
            icaoId AS station_id,
            timezone('America/Los_Angeles', to_timestamp(obsTime)) AS obs_time_local,
            CAST(timezone('America/Los_Angeles', to_timestamp(obsTime)) AS DATE) AS obs_date_local,
            temp AS temp_c,
            dewp AS dewpoint_c,
            rawOb,
            filename
        FROM raw_metar_all
        WHERE obsTime IS NOT NULL
          AND temp IS NOT NULL
          AND icaoId = 'KSFO'
        """)

        # =========================================================
        # 3) DEDUPLICATION
        # =========================================================
        # Remove duplicate observations (keep latest version by filename).
        con.sql("""
        CREATE OR REPLACE VIEW staging_metar_obs_local_dedup AS
        WITH ranked AS (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY station_id, obs_time_local
                    ORDER BY filename DESC
                ) AS rn
            FROM staging_metar_obs_local
        )
        SELECT
            station_id,
            obs_time_local,
            obs_date_local,
            temp_c,
            dewpoint_c,
            rawOb,
            filename
        FROM ranked
        WHERE rn = 1
        """)

        # =========================================================
        # 4) INTRADAY PROGRESSION FEATURES
        # =========================================================
        # Compute:
        #   - running max temperature during the day
        #   - final max temperature (ground truth)
        con.sql("""
        CREATE OR REPLACE VIEW mart_intraday_progression AS
        SELECT
            obs_date_local,
            EXTRACT(hour FROM obs_time_local) AS hour_local,
            MAX(temp_c) OVER (
                PARTITION BY obs_date_local
                ORDER BY obs_time_local
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS running_max_c,
            MAX(temp_c) OVER (
                PARTITION BY obs_date_local
            ) AS final_max_c
        FROM staging_metar_obs_local_dedup
        """)

        # Convert to Fahrenheit and compute "remaining delta".
        con.sql("""
        CREATE OR REPLACE VIEW mart_intraday_progression_f AS
        SELECT
            obs_date_local,
            hour_local,
            ROUND((running_max_c * 9.0/5.0) + 32, 2) AS running_max_f,
            ROUND((final_max_c * 9.0/5.0) + 32, 2) AS final_max_f,
            ROUND(((final_max_c - running_max_c) * 9.0/5.0), 2) AS remaining_delta_f
        FROM mart_intraday_progression
        """)

        # =========================================================
        # 5) HISTORICAL HOUR MODEL
        # =========================================================
        # Learn how much temperature typically increases after each hour.
        con.sql("""
        CREATE OR REPLACE VIEW weather_hour_model_hist AS
        SELECT
            hour_local,
            ROUND(AVG(remaining_delta_f), 2) AS avg_remaining,
            ROUND(APPROX_QUANTILE(remaining_delta_f, 0.25), 2) AS p25,
            ROUND(APPROX_QUANTILE(remaining_delta_f, 0.5), 2) AS median,
            ROUND(APPROX_QUANTILE(remaining_delta_f, 0.75), 2) AS p75,
            COUNT(*) AS samples
        FROM mart_intraday_progression_f
        GROUP BY 1
        """)

        # =========================================================
        # 6) RECENT 7-DAY MODEL
        # =========================================================
        # Capture short-term regime shifts (recent weather patterns).
        con.sql("""
        CREATE OR REPLACE VIEW weather_hour_model_7d AS
        SELECT
            hour_local,
            ROUND(AVG(remaining_delta_f), 2) AS avg_remaining,
            ROUND(APPROX_QUANTILE(remaining_delta_f, 0.25), 2) AS p25,
            ROUND(APPROX_QUANTILE(remaining_delta_f, 0.5), 2) AS median,
            ROUND(APPROX_QUANTILE(remaining_delta_f, 0.75), 2) AS p75,
            COUNT(*) AS samples
        FROM mart_intraday_progression_f
        WHERE obs_date_local >= (
            SELECT MAX(obs_date_local) - INTERVAL 7 DAY
            FROM mart_intraday_progression_f
        )
        GROUP BY 1
        HAVING COUNT(*) >= 5
        """)

        # =========================================================
        # 7) BUCKETED MODEL (BY CURRENT TEMP LEVEL)
        # =========================================================
        # Segment by current temperature regime (<60, 60–69, 70+).
        con.sql("""
        CREATE OR REPLACE VIEW weather_hour_bucket_model AS
        SELECT
            hour_local,
            CASE
                WHEN running_max_f < 60 THEN 0
                WHEN running_max_f < 70 THEN 1
                ELSE 2
            END AS bucket_id,
            CASE
                WHEN running_max_f < 60 THEN '<60'
                WHEN running_max_f < 70 THEN '60-69'
                ELSE '70+'
            END AS temp_bucket,
            ROUND(AVG(remaining_delta_f), 2) AS avg_remaining,
            ROUND(APPROX_QUANTILE(remaining_delta_f, 0.25), 2) AS p25,
            ROUND(APPROX_QUANTILE(remaining_delta_f, 0.5), 2) AS median,
            ROUND(APPROX_QUANTILE(remaining_delta_f, 0.75), 2) AS p75,
            COUNT(*) AS hist_samples
        FROM mart_intraday_progression_f
        GROUP BY 1, 2, 3
        HAVING COUNT(*) >= 10
        """)

        # =========================================================
        # 8) BLENDED MODEL (RECENCY ADJUSTMENT)
        # =========================================================
        # Adjust historical model using recent 7-day shift.
        con.sql("""
        CREATE OR REPLACE VIEW weather_hour_bucket_model_blended AS
        WITH hour_shift AS (
            SELECT
                h.hour_local,
                CASE
                    WHEN r.avg_remaining IS NOT NULL THEN r.avg_remaining - h.avg_remaining
                    ELSE 0
                END AS avg_shift,
                CASE
                    WHEN r.p25 IS NOT NULL THEN r.p25 - h.p25
                    ELSE 0
                END AS p25_shift,
                CASE
                    WHEN r.median IS NOT NULL THEN r.median - h.median
                    ELSE 0
                END AS median_shift,
                CASE
                    WHEN r.p75 IS NOT NULL THEN r.p75 - h.p75
                    ELSE 0
                END AS p75_shift
            FROM weather_hour_model_hist h
            LEFT JOIN weather_hour_model_7d r
                ON h.hour_local = r.hour_local
        )
        SELECT
            b.hour_local,
            b.bucket_id,
            b.temp_bucket,
            ROUND(b.avg_remaining + 0.3 * s.avg_shift, 2) AS avg_remaining,
            ROUND(b.p25 + 0.3 * s.p25_shift, 2) AS p25,
            ROUND(b.median + 0.3 * s.median_shift, 2) AS median,
            ROUND(b.p75 + 0.3 * s.p75_shift, 2) AS p75,
            b.hist_samples
        FROM weather_hour_bucket_model b
        LEFT JOIN hour_shift s
            ON b.hour_local = s.hour_local
        """)

        # =========================================================
        # 9) CURRENT-DAY KALSHI MARKETS
        # =========================================================
        # Load only today's markets (based on local date).
        con.sql("""
        CREATE OR REPLACE VIEW current_day_kalshi_market AS
        SELECT
            series_ticker,
            event_date,
            event_ticker,
            title,
            sub_title,
            category,
            mutually_exclusive,
            status,
            winning_market_ticker,
            winning_market_subtitle,
            collected_at_utc,

            market.ticker AS market_ticker,
            market.event_ticker AS market_event_ticker,
            market.subtitle AS market_subtitle,
            market.yes_sub_title,
            market.strike_type,
            market.floor_strike,
            market.cap_strike,
            market.status AS market_status,
            market.result AS market_result

        FROM (
            SELECT *
            FROM read_json_auto(
                's3://weather-kalshi-data/raw/kalshi/historical/market/KXHIGHTSFO/*.json',
                union_by_name=True,
                filename=True
            )
            WHERE event_date = CAST((CURRENT_TIMESTAMP AT TIME ZONE 'America/Los_Angeles') AS DATE)
        ) t,
        UNNEST(markets) AS m(market)
        """)

        # =========================================================
        # 10) FINAL LOOKUP TABLE
        # =========================================================
        # Cross join markets with weather model features.
        con.sql("""
        CREATE OR REPLACE VIEW current_day_market_weather_lookup AS
        SELECT
            k.event_date,
            k.event_ticker,
            k.market_ticker,
            k.yes_sub_title,
            k.strike_type,
            k.floor_strike,
            k.cap_strike,

            w.hour_local,
            w.bucket_id,
            w.temp_bucket,
            w.avg_remaining,
            w.p25,
            w.median,
            w.p75,
            w.hist_samples

        FROM current_day_kalshi_market k
        CROSS JOIN weather_hour_bucket_model_blended w
        ORDER BY k.market_ticker, w.hour_local, w.bucket_id
        """)

        # =========================================================
        # 11) EXPORT TO S3
        # =========================================================
        # Determine today's date dynamically.
        event_date = con.sql("""
        SELECT CAST((CURRENT_TIMESTAMP AT TIME ZONE 'America/Los_Angeles') AS DATE)
        """).fetchone()[0]

        s3_output_path = f"s3://weather-kalshi-data/processed/current_day_market/{event_date}.parquet"

        print(f"Writing to S3: {s3_output_path}")

        # Write final dataset as Parquet for efficient downstream usage.
        con.sql(f"""
        COPY (
            SELECT *
            FROM current_day_market_weather_lookup
            ORDER BY market_ticker, hour_local, bucket_id
        ) TO '{s3_output_path}'
        (FORMAT PARQUET);
        """)

        return s3_output_path

    finally:
        # Ensure DuckDB connection is always closed.
        con.close()


if __name__ == "__main__":
    out = build_current_day_market_weather_lookup()
    print(f"Wrote parquet to: {out}")