"""Event generator — creates realistic simulated Spotify listening events.

This module is responsible only for generating event payloads.
It has zero side-effects (no I/O, no network calls) making it
trivially testable.
"""

from __future__ import annotations

import json
import random
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

# ── Constants ────────────────────────────────────────────────────────

FIXTURES_DIR = Path(__file__).resolve().parent / "fixtures"
DEVICES: list[str] = ["mobile", "desktop", "web"]
COUNTRIES: list[str] = ["US", "UK", "CA", "AU", "IN", "DE"]
EVENT_TYPES: list[str] = ["play", "pause", "skip", "add_to_playlist"]


# ── Song Catalog ─────────────────────────────────────────────────────


def load_song_catalog(path: Path | None = None) -> list[dict[str, str]]:
    """Load and enrich the song catalog with deterministic song IDs.

    Each song gets a UUID5 derived from `artist::song` so the same
    combination always produces the same ID — important for downstream
    joins and deduplication.

    Args:
        path: Path to the songs JSON file. Defaults to
              ``fixtures/songs.json``.

    Returns:
        A list of dicts with keys: artist, song, song_id.
    """
    if path is None:
        path = FIXTURES_DIR / "songs.json"

    with open(path, encoding="utf-8") as f:
        pairs: list[dict[str, str]] = json.load(f)

    for pair in pairs:
        namespace_key = f"{pair['artist']}::{pair['song']}"
        pair["song_id"] = str(uuid.uuid5(uuid.NAMESPACE_DNS, namespace_key))

    return pairs


def generate_user_pool(count: int) -> list[str]:
    """Generate a fixed pool of user IDs.

    Args:
        count: Number of unique users to simulate.

    Returns:
        A list of UUID4 strings.
    """
    return [str(uuid.uuid4()) for _ in range(count)]


def generate_event(
    song_catalog: list[dict[str, str]],
    user_ids: list[str],
) -> dict[str, Any]:
    """Generate a single simulated Spotify listening event.

    Args:
        song_catalog: The enriched song catalog from ``load_song_catalog()``.
        user_ids: Pool of user UUIDs to sample from.

    Returns:
        A dictionary representing one event with keys:
        event_id, user_id, song_id, artist_name, song_name,
        event_type, device_type, country, timestamp.
    """
    pair = random.choice(song_catalog)
    return {
        "event_id": str(uuid.uuid4()),
        "user_id": random.choice(user_ids),
        "song_id": pair["song_id"],
        "artist_name": pair["artist"],
        "song_name": pair["song"],
        "event_type": random.choice(EVENT_TYPES),
        "device_type": random.choice(DEVICES),
        "country": random.choice(COUNTRIES),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
