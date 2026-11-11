"""Offline comparison of generated IDs, observed completions and receptions.

Missing IDs are computed against the publisher's actual recorded samples, not
its completion flags. An unconfirmed publication may have reached a subscriber.
Interruption times are the subscriber's observations, not service stop times.
"""

import csv
import json
import math
from collections import Counter
from datetime import datetime
from pathlib import Path

from experiments.qos.analysis import DEFAULT_OUTPUT_DIR, latency_statistics

from .common import iso_from_ns, new_run_id


def _positive_int(value: str) -> int:
    number = int(value)
    if number < 1:
        raise ValueError("expected a positive integer")
    return number


def _flag(value: str) -> bool:
    if value.lower() not in ("true", "false", "1", "0"):
        raise ValueError("expected a boolean flag")
    return value.lower() in ("true", "1")


def _timestamp(value: str) -> str:
    if datetime.fromisoformat(value).utcoffset() is None:
        raise ValueError("expected a timezone-aware ISO-8601 timestamp")
    return value


def _read(path: Path, required: set[str], convert) -> list[dict]:
    rows = []
    try:
        with Path(path).open(encoding="utf-8-sig", newline="") as stream:
            reader = csv.DictReader(stream, strict=True)
            fields = reader.fieldnames or []
            if not required.issubset(fields) or len(fields) != len(set(fields)):
                raise ValueError(f"{path}: required columns: {', '.join(sorted(required))}; headers must be unique")
            for row in reader:
                try:
                    if None in row or any(row[field] is None for field in required):
                        raise ValueError("invalid number of CSV fields")
                    rows.append(convert(row))
                except (ValueError, TypeError, OverflowError) as exc:
                    raise ValueError(f"{path}, CSV line {reader.line_num}: {exc}") from exc
    except (csv.Error, UnicodeError) as exc:
        raise ValueError(f"{path}: invalid CSV: {exc}") from exc
    return rows


def read_publisher(path: Path) -> list[dict]:
    def convert(row):
        attempted, success = _flag(row["publish_attempted"]), _flag(row["publish_success"])
        sent_ns = _positive_int(row["sent_at_unix_ns"]) if row["sent_at_unix_ns"] else None
        if (success and not attempted) or attempted != (sent_ns is not None):
            raise ValueError("inconsistent publication flags and sent_at_unix_ns")
        qos = int(row["qos"])
        if qos not in (0, 1, 2):
            raise ValueError("invalid QoS")
        return {
            "message_id": _positive_int(row["message_id"]), "generated_at": _timestamp(row["generated_at"]),
            "publish_attempted": attempted, "publish_success": success,
            "sent_at_unix_ns": sent_ns, "qos": qos, "publish_status": row.get("publish_status", ""),
        }

    rows = _read(path, {"message_id", "generated_at", "publish_attempted", "publish_success", "sent_at_unix_ns", "qos"}, convert)
    if len({row["message_id"] for row in rows}) != len(rows):
        raise ValueError(f"{path}: publisher must contain exactly one row per generated message_id")
    if len({row["qos"] for row in rows}) > 1:
        raise ValueError(f"{path}: mixed publisher QoS values")
    return rows


def read_subscriber(path: Path) -> list[dict]:
    def convert(row):
        latency = float(row["latency_ms"]) if row["latency_ms"] else None
        if latency is not None and not math.isfinite(latency):
            raise ValueError("latency_ms must be finite when available")
        return {
            "message_id": _positive_int(row["message_id"]),
            "received_at": _timestamp(row["received_at"]),
            "received_at_unix_ns": _positive_int(row["received_at_unix_ns"]), "latency_ms": latency,
        }

    return _read(path, {"message_id", "received_at", "received_at_unix_ns", "latency_ms"}, convert)


def read_events(path: Path) -> list[dict]:
    def convert(row):
        if row["role"] != "subscriber":
            raise ValueError("use the subscriber events CSV for recovery timing")
        return {"timestamp": _timestamp(row["timestamp"]), "unix_ns": _positive_int(row["unix_ns"]), "event": row["event"]}

    return _read(path, {"timestamp", "unix_ns", "role", "event"}, convert)


def interruption_metrics(events: list[dict], received: list[dict]) -> list[dict]:
    episodes = []
    connected_before = False
    outage_ns = None
    for event in sorted(events, key=lambda item: item["unix_ns"]):
        kind, now = event["event"], event["unix_ns"]
        if kind == "CONNECTED":
            connected_before = True
        elif kind == "DISCONNECTED" and connected_before and outage_ns is None:
            outage_ns = now
        elif kind == "RECONNECTED":
            if outage_ns is not None:
                episodes.append({"disconnected_ns": outage_ns, "reconnected_ns": now})
                outage_ns = None
            connected_before = True
    if outage_ns is not None:
        episodes.append({"disconnected_ns": outage_ns, "reconnected_ns": None})

    results = []
    for index, episode in enumerate(episodes):
        down, up = episode["disconnected_ns"], episode["reconnected_ns"]
        next_down = episodes[index + 1]["disconnected_ns"] if index + 1 < len(episodes) else None
        candidates = [
            row["received_at_unix_ns"] for row in received
            if up is not None and row["received_at_unix_ns"] >= up
            and (next_down is None or row["received_at_unix_ns"] < next_down)
        ]
        first = min(candidates) if candidates else None
        results.append({
            "disconnected_at": iso_from_ns(down), "reconnected_at": iso_from_ns(up) if up is not None else None,
            "interruption_duration_s": (up - down) / 1_000_000_000 if up is not None else None,
            "first_received_after_reconnect_at": iso_from_ns(first) if first is not None else None,
            "recovery_to_first_message_s": (first - up) / 1_000_000_000 if first is not None else None,
        })
    return results


def analyze_run(publisher_csv: Path, subscriber_csv: Path, events_csv: Path) -> dict:
    published = read_publisher(publisher_csv)
    received = read_subscriber(subscriber_csv)
    episodes = interruption_metrics(read_events(events_csv), received)
    generated_ids = {row["message_id"] for row in published}
    counts = Counter(row["message_id"] for row in received)
    missing = sorted(generated_ids - counts.keys())
    latencies = [row["latency_ms"] for row in received if row["latency_ms"] is not None]
    successful = sum(row["publish_success"] for row in published)
    first = episodes[0] if episodes else dict.fromkeys((
        "disconnected_at", "reconnected_at", "interruption_duration_s",
        "first_received_after_reconnect_at", "recovery_to_first_message_s",
    ))
    return {
        "publisher_csv": str(Path(publisher_csv).resolve()), "subscriber_csv": str(Path(subscriber_csv).resolve()),
        "events_csv": str(Path(events_csv).resolve()), "qos": published[0]["qos"] if published else None,
        "generated_samples": len(published),
        "publications_attempted": sum(row["publish_attempted"] for row in published),
        "publications_successful": successful,
        "publications_unsuccessful": len(published) - successful,
        "publications_not_attempted": sum(not row["publish_attempted"] for row in published),
        "publications_unconfirmed": sum(row["publish_status"] == "unconfirmed" for row in published),
        "received_messages": len(received), "unique_received_ids": len(counts),
        "missing_count": len(missing), "missing_ids": missing,
        "loss_pct": 100 * len(missing) / len(published) if published else None,
        "duplicates": len(received) - len(counts),
        "duplicate_ids": sorted(message_id for message_id, count in counts.items() if count > 1),
        "unexpected_received_ids": sorted(counts.keys() - generated_ids),
        "interruption_count": len(episodes), **first, "interruptions": episodes,
        "latency_samples": len(latencies), **latency_statistics(latencies),
    }


def write_summary(summary: dict, output_dir: Path = DEFAULT_OUTPUT_DIR) -> Path:
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / f"disconnection_summary_{new_run_id()}.csv"
    with path.open("x", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(summary))
        writer.writeheader()
        writer.writerow({key: json.dumps(value) if isinstance(value, list) else value for key, value in summary.items()})
    return path
