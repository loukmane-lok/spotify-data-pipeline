"""Kafka event producer — streams simulated Spotify events to a Kafka topic.

Entrypoint for the streaming producer. Run with:
    python -m src.producer.main

Features:
    - Graceful shutdown on SIGINT/SIGTERM (flushes pending messages)
    - Structured logging with configurable verbosity
    - Externalized configuration via environment variables
"""

from __future__ import annotations

import json
import logging
import signal
import sys
import time
from types import FrameType

from kafka import KafkaProducer
from kafka.errors import KafkaError

from src.config import get_kafka_config, get_simulator_config
from src.producer.event_generator import (
    generate_event,
    generate_user_pool,
    load_song_catalog,
)

logger = logging.getLogger(__name__)

# ── Globals for signal handler access ────────────────────────────────
_shutdown_requested: bool = False
_producer: KafkaProducer | None = None


def _handle_shutdown(signum: int, frame: FrameType | None) -> None:
    """Handle SIGINT/SIGTERM for graceful shutdown."""
    global _shutdown_requested
    sig_name = signal.Signals(signum).name
    logger.info("Received %s — initiating graceful shutdown...", sig_name)
    _shutdown_requested = True


def create_producer(bootstrap_servers: str) -> KafkaProducer:
    """Create and return a configured KafkaProducer.

    Args:
        bootstrap_servers: Kafka broker address(es).

    Returns:
        A configured KafkaProducer instance.

    Raises:
        KafkaError: If connection to the broker fails.
    """
    return KafkaProducer(
        bootstrap_servers=[bootstrap_servers],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        retries=3,
        linger_ms=10,
    )


def run() -> None:
    """Main producer loop — generates events and publishes to Kafka."""
    global _producer

    kafka_cfg = get_kafka_config()
    sim_cfg = get_simulator_config()

    # Register shutdown handlers
    signal.signal(signal.SIGINT, _handle_shutdown)
    signal.signal(signal.SIGTERM, _handle_shutdown)

    # Load data
    song_catalog = load_song_catalog()
    user_ids = generate_user_pool(sim_cfg.user_count)

    logger.info(
        "🎧 Starting Spotify event producer | topic=%s | songs=%d | users=%d",
        kafka_cfg.topic,
        len(song_catalog),
        len(user_ids),
    )
    for pair in song_catalog:
        logger.info(
            "  %s — %s → song_id=%s",
            pair["song"],
            pair["artist"],
            pair["song_id"],
        )

    # Connect to Kafka
    try:
        _producer = create_producer(kafka_cfg.bootstrap_servers)
    except KafkaError:
        logger.exception("Failed to connect to Kafka broker at %s", kafka_cfg.bootstrap_servers)
        sys.exit(1)

    # Produce events
    event_count = 0
    try:
        while not _shutdown_requested:
            event = generate_event(song_catalog, user_ids)
            _producer.send(kafka_cfg.topic, event)
            event_count += 1

            logger.info(
                "Produced event #%d: %s — %s by %s (user=%s)",
                event_count,
                event["event_type"],
                event["song_name"],
                event["artist_name"],
                event["user_id"][:8],
            )
            time.sleep(sim_cfg.event_interval_seconds)
    finally:
        pending = _producer.metrics().get("record-send-total", 0)
        logger.info("Flushing %d pending messages...", pending)
        _producer.flush(timeout=10)
        _producer.close(timeout=10)
        logger.info("✅ Producer shut down cleanly after %d events.", event_count)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    run()
