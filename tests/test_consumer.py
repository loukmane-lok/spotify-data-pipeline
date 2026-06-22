"""Tests for the consumer batch logic."""

from __future__ import annotations

import json
from unittest.mock import MagicMock

from src.consumer.main import flush_batch


class TestFlushBatch:
    """Tests for the S3 batch write function."""

    def test_empty_batch_does_nothing(self) -> None:
        """An empty batch should not call S3."""
        mock_s3 = MagicMock()
        flush_batch(mock_s3, "test-bucket", [])
        mock_s3.put_object.assert_not_called()

    def test_writes_ndjson_to_s3(self, sample_event: dict) -> None:
        """Should write events as newline-delimited JSON."""
        mock_s3 = MagicMock()
        batch = [sample_event, sample_event]

        flush_batch(mock_s3, "test-bucket", batch)

        mock_s3.put_object.assert_called_once()
        call_kwargs = mock_s3.put_object.call_args[1]

        assert call_kwargs["Bucket"] == "test-bucket"
        assert call_kwargs["Key"].startswith("bronze/date=")
        assert call_kwargs["Key"].endswith(".json")
        assert call_kwargs["ContentType"] == "application/x-ndjson"

        # Verify body is valid NDJSON
        body = call_kwargs["Body"].decode("utf-8")
        lines = body.strip().split("\n")
        assert len(lines) == 2
        for line in lines:
            parsed = json.loads(line)
            assert parsed["event_id"] == "evt-001"

    def test_s3_key_has_date_partition(self, sample_event: dict) -> None:
        """S3 key should contain date= and hour= partitions."""
        mock_s3 = MagicMock()
        flush_batch(mock_s3, "test-bucket", [sample_event])

        key = mock_s3.put_object.call_args[1]["Key"]
        assert "date=" in key
        assert "hour=" in key
