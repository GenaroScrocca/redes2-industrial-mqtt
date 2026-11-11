"""Decode transport payloads and build CSV records with reception latency."""

import json
import math
from datetime import datetime, timezone

SIGNAL_FIELDS = ("pressure_bar", "flow_lpm", "temperature_c", "vibration_mms", "rpm")
CSV_FIELDS = (
    "message_id", "asset_id", "timestamp", "received_at", "sent_at_unix_ns", "latency_ms",
    *SIGNAL_FIELDS,
)


def decode_telemetry(payload: bytes, received_ns: int) -> dict:
    """Validate required fields; malformed messages never reach metrics or CSV."""
    try:
        data = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, ValueError) as exc:
        raise ValueError(f"Invalid telemetry JSON: {exc}") from exc
    if not isinstance(data, dict):
        raise ValueError("Telemetry JSON must be an object")
    for name in ("message_id", "sent_at_unix_ns"):
        if type(data.get(name)) is not int or data[name] < 1:
            raise ValueError(f"{name} must be a positive integer")
    if not isinstance(data.get("asset_id"), str) or not data["asset_id"].strip():
        raise ValueError("asset_id must be a nonempty string")
    try:
        timestamp = datetime.fromisoformat(data["timestamp"])
        if timestamp.utcoffset() is None:
            raise ValueError("timestamp must have a timezone")
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError("timestamp must be a timezone-aware ISO-8601 string") from exc
    for name in SIGNAL_FIELDS:
        value = data.get(name)
        if type(value) not in (int, float) or not math.isfinite(value):
            raise ValueError(f"{name} must be a finite number")

    seconds, nanoseconds = divmod(received_ns, 1_000_000_000)
    received_at = datetime.fromtimestamp(seconds, timezone.utc).replace(microsecond=nanoseconds // 1000)
    return {
        "message_id": data["message_id"],
        "asset_id": data["asset_id"],
        "timestamp": data["timestamp"],
        "received_at": received_at.isoformat(timespec="microseconds"),
        "sent_at_unix_ns": data["sent_at_unix_ns"],
        "latency_ms": (received_ns - data["sent_at_unix_ns"]) / 1_000_000,
        **{name: data[name] for name in SIGNAL_FIELDS},
    }
