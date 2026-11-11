"""Resubscribe after recovery and keep recording into the same run files."""

import argparse
import json
import math
import sys
import time
from datetime import datetime
from queue import Empty

from src.messaging.cli import mqtt_config
from src.messaging.telemetry import CSV_FIELDS, SIGNAL_FIELDS

from .client import ResilientClient
from .common import CsvJournal, EVENT_FIELDS, add_experiment_arguments, drain_events, iso_from_ns, run_paths

SUBSCRIBER_FIELDS = (*CSV_FIELDS, "received_at_unix_ns", "is_duplicate")


def decode_received(payload: bytes, received_ns: int) -> dict:
    data = json.loads(payload.decode("utf-8"))
    if not isinstance(data, dict) or type(data.get("message_id")) is not int or data["message_id"] < 1:
        raise ValueError("message_id must be a positive integer in a JSON object")
    if not isinstance(data.get("asset_id"), str) or not data["asset_id"].strip():
        raise ValueError("asset_id must be a nonempty string")
    if not isinstance(data.get("timestamp"), str) or datetime.fromisoformat(data["timestamp"]).utcoffset() is None:
        raise ValueError("timestamp must be timezone-aware ISO-8601")
    for name in SIGNAL_FIELDS:
        if type(data.get(name)) not in (int, float) or not math.isfinite(data[name]):
            raise ValueError(f"{name} must be finite")
    sent_ns = data.get("sent_at_unix_ns")
    if sent_ns is not None and (type(sent_ns) is not int or sent_ns < 1):
        raise ValueError("sent_at_unix_ns must be a positive integer when present")
    return {
        "message_id": data["message_id"], "asset_id": data["asset_id"], "timestamp": data["timestamp"],
        "received_at": iso_from_ns(received_ns), "received_at_unix_ns": received_ns,
        "sent_at_unix_ns": sent_ns,
        "latency_ms": (received_ns - sent_ns) / 1_000_000 if sent_ns is not None else None,
        **{name: data[name] for name in SIGNAL_FIELDS},
    }


class ReceptionLog:
    def __init__(self, journal: CsvJournal) -> None:
        self.journal = journal
        self.ids: set[int] = set()
        self.received = self.duplicates = self.invalid = 0

    def collect(self, session: ResilientClient) -> None:
        while True:
            try:
                payload, received_ns = session.messages.get_nowait()
            except Empty:
                return
            try:
                row = decode_received(payload, received_ns)
            except (ValueError, UnicodeError, OverflowError) as exc:
                self.invalid += 1
                print(f"Mensaje descartado: {exc}", file=sys.stderr)
                continue
            row["is_duplicate"] = row["message_id"] in self.ids
            self.duplicates += int(row["is_duplicate"])
            self.ids.add(row["message_id"])
            self.received += 1
            self.journal.write(row)
            latency = "N/A" if row["latency_ms"] is None else f"{row['latency_ms']:.3f}"
            print(
                f"RECEIVED message_id={row['message_id']} asset_id={row['asset_id']} "
                f"latency_ms={latency} duplicate={int(row['is_duplicate'])}", flush=True,
            )


def run_subscriber(config, *, retry_interval, paths) -> int:
    session = ResilientClient(config, role="subscriber", retry_interval=retry_interval)
    with CsvJournal(paths[0], SUBSCRIBER_FIELDS) as journal, CsvJournal(paths[1], EVENT_FIELDS) as events:
        print(f"Subscriber CSV={paths[0]}\nSubscriber events CSV={paths[1]}", flush=True)
        log = ReceptionLog(journal)
        try:
            session.start()
            while True:
                drain_events(session, events)
                log.collect(session)
                if session.error:
                    raise RuntimeError(session.error)
                time.sleep(0.05)
        except KeyboardInterrupt:
            print("\nSubscriber experimental detenido.")
        finally:
            try:
                session.stop()
            finally:
                log.collect(session)
                drain_events(session, events)
                print(f"Recibidos={log.received} IDs_unicos={len(log.ids)} duplicados={log.duplicates} invalidos={log.invalid}")
    return 0


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Experimental subscriber for a manual broker interruption.")
    add_experiment_arguments(parser)
    args = parser.parse_args(argv)
    config = mqtt_config(parser, args)
    try:
        return run_subscriber(config, retry_interval=args.retry_interval, paths=run_paths(args, "subscriber"))
    except (OSError, ValueError, RuntimeError) as exc:
        print(f"Error subscriber experimental: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
