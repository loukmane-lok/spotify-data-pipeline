"""Shared test fixtures for the Spotify Data Pipeline test suite."""

from __future__ import annotations

import pytest


@pytest.fixture
def sample_song_catalog() -> list[dict[str, str]]:
    """A minimal song catalog for testing."""
    return [
        {
            "artist": "Test Artist",
            "song": "Test Song",
            "song_id": "test-song-id-001",
        },
        {
            "artist": "Another Artist",
            "song": "Another Song",
            "song_id": "test-song-id-002",
        },
    ]


@pytest.fixture
def sample_user_ids() -> list[str]:
    """A small pool of user UUIDs for testing."""
    return [
        "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
        "11111111-2222-3333-4444-555555555555",
    ]


@pytest.fixture
def sample_event() -> dict[str, str]:
    """A single well-formed event for testing."""
    return {
        "event_id": "evt-001",
        "user_id": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
        "song_id": "test-song-id-001",
        "artist_name": "Test Artist",
        "song_name": "Test Song",
        "event_type": "play",
        "device_type": "mobile",
        "country": "US",
        "timestamp": "2024-06-01T12:00:00+00:00",
    }
