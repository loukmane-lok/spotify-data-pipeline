"""Kafka → S3 consumer — writes events in micro-batches to the bronze layer.

Listens to a Kafka topic and writes events as newline-delimited JSON
files to S3, partitioned by date and hour.

Run with:
    python -m src.consumer.main

Features:
    - Micro-batch writes (configurable batch size)
    - Partitioned S3 storage: ``bronze/date=YYYY-MM-DD/hour=HH/``
    - Graceful shutdown — flushes partial batch on SIGINT/SIGTERM
    - Structured logging
"""

from __future__ import annotations

import json
import logging
import signal
import sys
from datetime import datetime, timezone
from types import FrameType

import boto3
from botocore.exceptions import ClientError
from kafka import KafkaConsumer
from kafka.errors import KafkaError

from src.config import get_kafka_config, get_s3_config

logger = logging.getLogger(__name__)

# ── Globals for signal handler ───────────────────────────────────────
_shutdown_requested: bool = False


def _handle_shutdown(signum: int, frame: FrameType | None) -> None:
    """Handle SIGINT/SIGTERM for graceful shutdown."""
    global _shutdown_requested
    sig_name = signal.Signals(signum).name
    logger.info("Received %s — will flush and exit after current batch.", sig_name)
    _shutdown_requested = True


def create_s3_client(
    region: str,
    access_key_id: str,
    secret_access_key: str,
) -> boto3.client:
    """Create an authenticated S3 client.

    Args:
        region: AWS region.
        access_key_id: AWS access key ID.
        secret_access_key: AWS secret access key.

    Returns:
        A configured boto3 S3 client.
    """
    return boto3.client(
        "s3",
        region_name=region,
        aws_access_key_id=access_key_id,
        aws_secret_access_key=secret_access_key,
    )


def ensure_bucket_exists(s3_client: boto3.client, bucket_name: str, region: str) -> None:
    """Verify the S3 bucket exists, creating it if necessary.

    Args:
        s3_client: Authenticated boto3 S3 client.
        bucket_name: Name of the target S3 bucket.
        region: AWS region for bucket creation.

    Raises:
        ClientError: If bucket access fails for reasons other than 404.
    """
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        logger.info("S3 bucket '%s' verified.", bucket_name)
    except ClientError as e:
        error_code = int(e.response["Error"]["Code"])
        if error_code == 404:
            logger.info("Bucket '%s' not found — creating...", bucket_name)
            create_params: dict = {"Bucket": bucket_name}
            if region != "us-east-1":
                create_params["CreateBucketConfiguration"] = {"LocationConstraint": region}
            s3_client.create_bucket(**create_params)
            logger.info("Bucket '%s' created in %s.", bucket_name, region)
        else:
            logger.exception("Failed to access bucket '%s'.", bucket_name)
            raise


def flush_batch(
    s3_client: boto3.client,
    bucket_name: str,
    batch: list[dict],
) -> None:
    """Write a batch of events to S3 as a newline-delimited JSON file.

    File path format: ``bronze/date=YYYY-MM-DD/hour=HH/<timestamp>.json``

    Args:
        s3_client: Authenticated boto3 S3 client.
        bucket_name: Target S3 bucket.
        batch: List of event dicts to write.
    """
    if not batch:
        return

    now = datetime.now(timezone.utc)
    date_path = now.strftime("date=%Y-%m-%d/hour=%H")
    file_name = f"spotify_events_{now.strftime('%Y-%m-%dT%H-%M-%S')}.json"
    file_path = f"bronze/{date_path}/{file_name}"

    json_data = "\n".join(json.dumps(event) for event in batch)

    s3_client.put_object(
        Bucket=bucket_name,
        Key=file_path,
        Body=json_data.encode("utf-8"),
        ContentType="application/x-ndjson",
    )

    logger.info(
        "Wrote %d events to s3://%s/%s",
        len(batch),
        bucket_name,
        file_path,
    )


def run() -> None:
    """Main consumer loop — reads from Kafka and micro-batches to S3."""
    kafka_cfg = get_kafka_config()
    s3_cfg = get_s3_config()

    # Register shutdown handlers
    signal.signal(signal.SIGINT, _handle_shutdown)
    signal.signal(signal.SIGTERM, _handle_shutdown)

    # Initialize S3
    s3_client = create_s3_client(
        region=s3_cfg.region,
        access_key_id=s3_cfg.access_key_id,
        secret_access_key=s3_cfg.secret_access_key,
    )
    ensure_bucket_exists(s3_client, s3_cfg.bucket_name, s3_cfg.region)

    # Initialize Kafka consumer
    try:
        consumer = KafkaConsumer(
            kafka_cfg.topic,
            bootstrap_servers=[kafka_cfg.bootstrap_servers],
            auto_offset_reset="earliest",
            group_id=kafka_cfg.group_id,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            consumer_timeout_ms=1000,  # Allows periodic shutdown checks
        )
    except KafkaError:
        logger.exception("Failed to connect to Kafka at %s", kafka_cfg.bootstrap_servers)
        sys.exit(1)

    logger.info(
        "📦 Listening for events on topic '%s' | batch_size=%d",
        kafka_cfg.topic,
        kafka_cfg.batch_size,
    )

    batch: list[dict] = []
    total_written = 0

    try:
        while not _shutdown_requested:
            # Poll with timeout so we can check shutdown flag
            messages = consumer.poll(timeout_ms=1000)
            for _tp, records in messages.items():
                for record in records:
                    batch.append(record.value)

                    if len(batch) >= kafka_cfg.batch_size:
                        flush_batch(s3_client, s3_cfg.bucket_name, batch)
                        total_written += len(batch)
                        batch = []

            if _shutdown_requested:
                break
    finally:
        # Flush any remaining events in the partial batch
        if batch:
            logger.info("Flushing partial batch of %d events...", len(batch))
            flush_batch(s3_client, s3_cfg.bucket_name, batch)
            total_written += len(batch)

        consumer.close()
        logger.info(
            "✅ Consumer shut down cleanly. Total events written: %d",
            total_written,
        )


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    run()
