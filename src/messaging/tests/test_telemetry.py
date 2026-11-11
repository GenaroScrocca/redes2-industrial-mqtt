import csv
import json
from datetime import datetime, timezone

import pytest

from src.messaging.metrics import TelemetryMetrics
from src.messaging.recording import CsvRecorder
from src.messaging.telemetry import CSV_FIELDS, SIGNAL_FIELDS, decode_telemetry
from src.simulator import PumpSimulator

RECEIVED_NS = 1_794_420_000_123_456_789


def payload():
    return {**PumpSimulator(seed=42).next_sample(), "sent_at_unix_ns": RECEIVED_NS - 2_500_000}


def test_decode_computes_latency_and_utc_received_at():
    data = payload()
    row = decode_telemetry(json.dumps(data).encode(), RECEIVED_NS)
    assert set(row) == set(CSV_FIELDS)
    assert row["latency_ms"] == 2.5
    assert row["sent_at_unix_ns"] == data["sent_at_unix_ns"]
    received_at = datetime.fromisoformat(row["received_at"])
    assert received_at.tzinfo == timezone.utc
    assert received_at.microsecond == 123456
    for name in SIGNAL_FIELDS:
        assert row[name] == data[name]


@pytest.mark.parametrize("encoded", [b"invalid", b"\xff", b"[]", b"null"])
def test_invalid_json_is_rejected(encoded):
    with pytest.raises(ValueError):
        decode_telemetry(encoded, RECEIVED_NS)


@pytest.mark.parametrize("field", ["message_id", "asset_id", "timestamp", "sent_at_unix_ns", *SIGNAL_FIELDS])
def test_missing_fields_are_rejected(field):
    data = payload()
    del data[field]
    with pytest.raises(ValueError):
        decode_telemetry(json.dumps(data).encode(), RECEIVED_NS)


@pytest.mark.parametrize(
    "field,value",
    [
        ("message_id", True), ("message_id", 0), ("sent_at_unix_ns", "123"),
        ("timestamp", "2026-11-11T18:00:00"), ("timestamp", 123), ("asset_id", ""),
        ("pressure_bar", float("nan")), ("flow_lpm", float("inf")),
        ("temperature_c", "60"), ("vibration_mms", True),
    ],
)
def test_invalid_field_types_and_values_are_rejected(field, value):
    data = payload()
    data[field] = value
    with pytest.raises(ValueError):
        decode_telemetry(json.dumps(data).encode(), RECEIVED_NS)


def test_future_sender_time_is_not_silently_clamped():
    data = payload()
    data["sent_at_unix_ns"] = RECEIVED_NS + 1_000_000
    assert decode_telemetry(json.dumps(data).encode(), RECEIVED_NS)["latency_ms"] == -1.0


def test_metrics_count_duplicates_and_average_all_valid_messages():
    metrics = TelemetryMetrics()
    for message_id, latency in [(1, 2.0), (1, 4.0), (1, 6.0), (2, 8.0)]:
        metrics.record({"message_id": message_id, "latency_ms": latency})
    metrics.record_invalid()
    assert metrics.total_received == 5
    assert metrics.invalid_messages == 1
    assert metrics.unique_ids == {1, 2}
    assert metrics.duplicate_ids == {1}
    assert metrics.duplicate_count == 2
    assert metrics.latency_min == 2.0
    assert metrics.latency_max == 8.0
    assert metrics.latency_average == 5.0
    assert "IDs duplicados: [1]" in metrics.summary()


def test_empty_metrics_summary_has_no_division_by_zero():
    metrics = TelemetryMetrics()
    assert metrics.latency_average is None
    assert metrics.summary().count("N/A") == 3


def test_csv_contains_required_fields_flushes_rows_and_does_not_overwrite(tmp_path):
    row = decode_telemetry(json.dumps(payload()).encode(), RECEIVED_NS)
    with CsvRecorder(tmp_path) as first:
        first.write(row)
        first.write(row)
        with first.path.open(encoding="utf-8", newline="") as stream:
            reader = csv.DictReader(stream)
            assert reader.fieldnames == list(CSV_FIELDS)
            rows = list(reader)
        assert len(rows) == 2
        assert rows[0]["latency_ms"] == "2.5"
        assert rows[0]["sent_at_unix_ns"] == str(row["sent_at_unix_ns"])
    with CsvRecorder(tmp_path) as second:
        second.write(row)
    assert first.path != second.path
    assert len(list(tmp_path.glob("*.csv"))) == 2
    assert first._stream.closed and second._stream.closed
