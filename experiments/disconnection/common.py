"""Experiment CSVs, clocks and shared CLI options; no broker service control."""

import csv
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from queue import Empty
from uuid import uuid4

from src.messaging.cli import add_mqtt_arguments, positive_int
from src.messaging.config import RAW_RESULTS_DIR

EVENT_FIELDS = ("timestamp", "unix_ns", "role", "event", "detail", "qos", "topic")
PUBLISHER_FIELDS = (
    "message_id", "generated_at", "publish_attempted", "publish_success",
    "sent_at_unix_ns", "qos", "publish_status", "publish_return_code", "completed_at", "detail",
)


def iso_from_ns(unix_ns: int) -> str:
    seconds, nanoseconds = divmod(unix_ns, 1_000_000_000)
    return datetime.fromtimestamp(seconds, timezone.utc).replace(
        microsecond=nanoseconds // 1000
    ).isoformat(timespec="microseconds")


def new_run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S") + "_" + uuid4().hex[:12]


def valid_run_id(value: str) -> str:
    if not re.fullmatch(r"[A-Za-z0-9_-]+", value):
        raise ValueError("run-id must contain only letters, digits, underscores or hyphens")
    return value


def add_experiment_arguments(parser) -> None:
    add_mqtt_arguments(parser)
    parser.add_argument("--retry-interval", type=positive_int, default=1, help="fixed reconnect delay in seconds")
    parser.add_argument("--run-id", default=None, help="shared run identifier; use a new value each run")
    parser.add_argument("--output-dir", type=Path, default=RAW_RESULTS_DIR)


def run_paths(args, role: str) -> tuple[Path, Path]:
    run_id = valid_run_id(args.run_id or new_run_id())
    prefix = args.output_dir / f"disconnection_{run_id}_{role}"
    return Path(f"{prefix}.csv"), Path(f"{prefix}_events.csv")


class CsvJournal:
    """Append final records on the main thread; refuse to overwrite a run."""

    def __init__(self, path: Path, fields) -> None:
        self.path = path
        path.parent.mkdir(parents=True, exist_ok=True)
        self._stream = path.open("x", encoding="utf-8", newline="")
        try:
            self._writer = csv.DictWriter(self._stream, fieldnames=fields)
            self._writer.writeheader()
            self._stream.flush()
        except BaseException:
            self._stream.close()
            raise

    def write(self, row: dict) -> None:
        self._writer.writerow(row)
        self._stream.flush()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self._stream.close()


def drain_events(session, journal: CsvJournal) -> None:
    while True:
        try:
            row = session.events.get_nowait()
        except Empty:
            return
        journal.write(row)
        print(f"{row['timestamp']} {row['role']} {row['event']} {row['detail']}", flush=True)


def wait_until(deadline: float) -> None:
    remaining = deadline - time.monotonic()
    if remaining > 0:
        time.sleep(remaining)
