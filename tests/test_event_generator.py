"""Tests for the event generator module."""

from __future__ import annotations

from src.producer.event_generator import (
    COUNTRIES,
    DEVICES,
    EVENT_TYPES,
    generate_event,
    generate_user_pool,
    load_song_catalog,
)


class TestLoadSongCatalog:
    """Tests for the song catalog loader."""

    def test_loads_default_catalog(self) -> None:
        """Should load the default songs.json fixture."""
        catalog = load_song_catalog()
        assert len(catalog) > 0

    def test_each_song_has_required_keys(self) -> None:
        """Each song dict must have artist, song, and song_id."""
        catalog = load_song_catalog()
        for item in catalog:
            assert "artist" in item
            assert "song" in item
            assert "song_id" in item

    def test_song_ids_are_deterministic(self) -> None:
        """Same input should produce the same song_id on every load."""
        catalog_1 = load_song_catalog()
        catalog_2 = load_song_catalog()
        for a, b in zip(catalog_1, catalog_2, strict=True):
            assert a["song_id"] == b["song_id"]

    def test_song_ids_are_unique(self) -> None:
        """All song IDs should be unique."""
        catalog = load_song_catalog()
        ids = [s["song_id"] for s in catalog]
        assert len(ids) == len(set(ids))


class TestGenerateUserPool:
    """Tests for user pool generation."""

    def test_correct_count(self) -> None:
        """Should generate the requested number of user IDs."""
        users = generate_user_pool(5)
        assert len(users) == 5

    def test_unique_ids(self) -> None:
        """All user IDs should be unique."""
        users = generate_user_pool(100)
        assert len(users) == len(set(users))

    def test_zero_users(self) -> None:
        """Should return empty list for zero users."""
        assert generate_user_pool(0) == []


class TestGenerateEvent:
    """Tests for single event generation."""

    def test_has_all_required_fields(
        self,
        sample_song_catalog: list[dict[str, str]],
        sample_user_ids: list[str],
    ) -> None:
        """Generated event must contain all expected keys."""
        event = generate_event(sample_song_catalog, sample_user_ids)

        required_keys = {
            "event_id",
            "user_id",
            "song_id",
            "artist_name",
            "song_name",
            "event_type",
            "device_type",
            "country",
            "timestamp",
        }
        assert set(event.keys()) == required_keys

    def test_event_type_is_valid(
        self,
        sample_song_catalog: list[dict[str, str]],
        sample_user_ids: list[str],
    ) -> None:
        """Event type must be one of the allowed values."""
        event = generate_event(sample_song_catalog, sample_user_ids)
        assert event["event_type"] in EVENT_TYPES

    def test_device_type_is_valid(
        self,
        sample_song_catalog: list[dict[str, str]],
        sample_user_ids: list[str],
    ) -> None:
        """Device type must be one of the allowed values."""
        event = generate_event(sample_song_catalog, sample_user_ids)
        assert event["device_type"] in DEVICES

    def test_country_is_valid(
        self,
        sample_song_catalog: list[dict[str, str]],
        sample_user_ids: list[str],
    ) -> None:
        """Country must be one of the allowed values."""
        event = generate_event(sample_song_catalog, sample_user_ids)
        assert event["country"] in COUNTRIES

    def test_user_id_from_pool(
        self,
        sample_song_catalog: list[dict[str, str]],
        sample_user_ids: list[str],
    ) -> None:
        """User ID must come from the provided pool."""
        event = generate_event(sample_song_catalog, sample_user_ids)
        assert event["user_id"] in sample_user_ids

    def test_song_from_catalog(
        self,
        sample_song_catalog: list[dict[str, str]],
        sample_user_ids: list[str],
    ) -> None:
        """Song details must come from the provided catalog."""
        event = generate_event(sample_song_catalog, sample_user_ids)
        song_ids = [s["song_id"] for s in sample_song_catalog]
        assert event["song_id"] in song_ids

    def test_timestamp_is_iso_format(
        self,
        sample_song_catalog: list[dict[str, str]],
        sample_user_ids: list[str],
    ) -> None:
        """Timestamp should be parseable ISO 8601."""
        from datetime import datetime

        event = generate_event(sample_song_catalog, sample_user_ids)
        # Should not raise
        datetime.fromisoformat(event["timestamp"])
