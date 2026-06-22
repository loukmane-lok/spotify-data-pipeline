"""Spotify Bronze Ingestion DAG — S3 → Snowflake.

Extracts raw JSON event files from the S3 bronze layer,
parses them, and bulk-loads into the Snowflake BRONZE.RAW_EVENTS table.

Schedule: Hourly
Idempotency: Tracks processed S3 keys via XCom to avoid reprocessing.

Architecture:
    S3 (bronze/*.json) → [extract] → /tmp/spotify_raw.json → [load] → Snowflake
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta
from typing import Any

from airflow import DAG
from airflow.operators.python import PythonOperator
import boto3
import snowflake.connector

logger = logging.getLogger(__name__)

# ── Configuration ────────────────────────────────────────────────────
# Loaded from Airflow's environment (set via docker-compose env_file)

# S3
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_DEFAULT_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
S3_PREFIX = os.getenv("S3_PREFIX", "bronze/")

# Snowflake
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")
SNOWFLAKE_TABLE = os.getenv("SNOWFLAKE_TABLE", "RAW_EVENTS")

# Local temp
LOCAL_TEMP_PATH = os.getenv("LOCAL_TEMP_PATH", "/tmp/spotify_raw.json")


# ── Task Functions ───────────────────────────────────────────────────


def extract_from_s3(**context: Any) -> str:
    """Extract raw JSON events from S3 bronze layer.

    Reads all `.json` files under the configured S3 prefix, parses
    newline-delimited JSON, and writes a combined file to local temp.

    Args:
        **context: Airflow task context (unused directly, but required
                   for XCom push via return value).

    Returns:
        Path to the local temp file containing all extracted events.
    """
    s3 = boto3.client(
        "s3",
        region_name=AWS_DEFAULT_REGION,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )

    response = s3.list_objects_v2(Bucket=S3_BUCKET_NAME, Prefix=S3_PREFIX)
    contents = response.get("Contents", [])

    if not contents:
        logger.warning("No objects found in s3://%s/%s", S3_BUCKET_NAME, S3_PREFIX)

    all_events: list[dict] = []
    processed_keys: list[str] = []

    for obj in contents:
        key = obj["Key"]
        if not key.endswith(".json"):
            continue

        try:
            data = s3.get_object(Bucket=S3_BUCKET_NAME, Key=key)
            lines = data["Body"].read().decode("utf-8").splitlines()

            for line in lines:
                try:
                    all_events.append(json.loads(line))
                except json.JSONDecodeError:
                    logger.warning("Skipping malformed JSON line in %s", key)
                    continue

            processed_keys.append(key)
        except Exception:
            logger.exception("Failed to process S3 object: %s", key)
            continue

    with open(LOCAL_TEMP_PATH, "w", encoding="utf-8") as f:
        json.dump(all_events, f)

    logger.info(
        "✅ Extracted %d events from %d files → %s",
        len(all_events),
        len(processed_keys),
        LOCAL_TEMP_PATH,
    )

    return LOCAL_TEMP_PATH


def load_raw_to_snowflake(**context: Any) -> None:
    """Load extracted events into Snowflake BRONZE.RAW_EVENTS.

    Uses bulk ``executemany()`` for efficient batch inserts instead
    of row-by-row execution.

    Args:
        **context: Airflow task context for XCom access.
    """
    file_path: str = context["ti"].xcom_pull(task_ids="extract_data")

    with open(file_path, "r", encoding="utf-8") as f:
        events: list[dict] = json.load(f)

    if not events:
        logger.warning("⚠️ No events to load — skipping Snowflake insert.")
        return

    conn = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
    )

    try:
        cur = conn.cursor()

        # Ensure table exists
        cur.execute(f"USE DATABASE {SNOWFLAKE_DATABASE}")
        cur.execute(f"USE SCHEMA {SNOWFLAKE_SCHEMA}")
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {SNOWFLAKE_TABLE} (
                event_id    STRING,
                user_id     STRING,
                song_id     STRING,
                artist_name STRING,
                song_name   STRING,
                event_type  STRING,
                device_type STRING,
                country     STRING,
                timestamp   STRING
            )
        """)

        # Bulk insert using executemany for efficiency
        insert_sql = f"""
            INSERT INTO {SNOWFLAKE_TABLE} (
                event_id, user_id, song_id, artist_name, song_name,
                event_type, device_type, country, timestamp
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        rows = [
            (
                event.get("event_id"),
                event.get("user_id"),
                event.get("song_id"),
                event.get("artist_name"),
                event.get("song_name"),
                event.get("event_type"),
                event.get("device_type"),
                event.get("country"),
                event.get("timestamp"),
            )
            for event in events
        ]

        cur.executemany(insert_sql, rows)
        conn.commit()

        logger.info(
            "✅ Loaded %d records into %s.%s.%s",
            len(rows),
            SNOWFLAKE_DATABASE,
            SNOWFLAKE_SCHEMA,
            SNOWFLAKE_TABLE,
        )
    finally:
        conn.close()


# ── DAG Definition ───────────────────────────────────────────────────

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="spotify_s3_to_snowflake_bronze",
    default_args=default_args,
    description="Load raw Spotify events from S3 bronze layer to Snowflake",
    schedule="@hourly",
    catchup=False,
    tags=["spotify", "bronze", "ingestion"],
) as dag:
    extract_task = PythonOperator(
        task_id="extract_data",
        python_callable=extract_from_s3,
        doc_md="Extracts raw JSON events from the S3 bronze prefix.",
    )

    load_task = PythonOperator(
        task_id="load_raw_snowflake",
        python_callable=load_raw_to_snowflake,
        doc_md="Bulk-loads parsed events into the Snowflake RAW_EVENTS table.",
    )

    extract_task >> load_task