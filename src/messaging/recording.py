"""One exclusive CSV file per subscriber run, flushed after every message."""

import csv
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from .config import RAW_RESULTS_DIR
from .telemetry import CSV_FIELDS


class CsvRecorder:
    def __init__(self, directory: Path = RAW_RESULTS_DIR) -> None:
        directory.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S_%fZ")
        self.path = directory / f"telemetry_{stamp}_{uuid4().hex}.csv"
        self._stream = self.path.open("x", encoding="utf-8", newline="")
        try:
            self._writer = csv.DictWriter(self._stream, fieldnames=CSV_FIELDS)
            self._writer.writeheader()
            self._stream.flush()
        except BaseException:
            self._stream.close()
            raise

    def write(self, row: dict) -> None:
        self._writer.writerow(row)
        self._stream.flush()

    def __enter__(self) -> "CsvRecorder":
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self._stream.close()
