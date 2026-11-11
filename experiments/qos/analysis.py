"""Analyze received CSV rows without running publishers or generating telemetry.

Loss uses missing IDs in 1..expected_messages for each run. Duplicates are
extra rows for the same ID within that run. Latency statistics include every
received row, including duplicates; aggregate quantiles pool the original
latencies rather than averaging per-run quantiles. P95 uses linear interpolation
at position 0.95 * (n - 1) in the sorted samples.
"""

import csv
import json
import math
import re
from collections import Counter
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from statistics import fmean, median
from uuid import uuid4

DEFAULT_EXPECTED_MESSAGES = 500
DEFAULT_OUTPUT_DIR = Path(__file__).resolve().parents[2] / "results" / "processed"
LATENCY_FIELDS = (
    "latency_min_ms", "latency_max_ms", "latency_mean_ms", "latency_median_ms", "latency_p95_ms",
)
RUN_FIELDS = (
    "csv_file", "qos", "run", "expected_messages", "received_rows", "unique_ids",
    "missing_count", "missing_ids", "loss_pct", "duplicates", "duplicate_ids",
    "unexpected_count", "unexpected_ids", *LATENCY_FIELDS,
)
AGGREGATE_FIELDS = (
    "qos", "run_count", "runs_with_latency", "expected_messages", "received_rows",
    "unique_ids", "missing_count", "loss_pct", "duplicates", "unexpected_count",
    *LATENCY_FIELDS, "latency_mean_of_run_means_ms",
)


def detect_run(path: Path) -> tuple[int, int]:
    """Read one qos0/qos1 token and one positive runN token from a CSV name."""
    qos_tokens = re.findall(r"(?:^|[_-])qos(\d+)(?=$|[_-])", path.stem, re.IGNORECASE)
    run_tokens = re.findall(r"(?:^|[_-])run(\d+)(?=$|[_-])", path.stem, re.IGNORECASE)
    if (
        path.suffix.lower() != ".csv"
        or len(qos_tokens) != 1
        or len(run_tokens) != 1
        or int(qos_tokens[0]) not in (0, 1)
        or int(run_tokens[0]) < 1
    ):
        raise ValueError(f"{path.name}: expected a CSV filename containing qos0/qos1 and runN")
    return int(qos_tokens[0]), int(run_tokens[0])


def latency_statistics(values: Iterable[float]) -> dict[str, float | None]:
    ordered = sorted(values)
    if not ordered:
        return dict.fromkeys(LATENCY_FIELDS)
    position = 0.95 * (len(ordered) - 1)
    lower, upper = math.floor(position), math.ceil(position)
    p95 = ordered[lower] + (ordered[upper] - ordered[lower]) * (position - lower)
    return {
        "latency_min_ms": ordered[0],
        "latency_max_ms": ordered[-1],
        "latency_mean_ms": fmean(ordered),
        "latency_median_ms": median(ordered),
        "latency_p95_ms": p95,
    }


@dataclass(frozen=True)
class RunAnalysis:
    csv_file: Path
    qos: int
    run: int
    expected_messages: int
    message_ids: tuple[int, ...]
    latencies_ms: tuple[float, ...]

    def summary(self) -> dict:
        counts = Counter(self.message_ids)
        missing = sorted(set(range(1, self.expected_messages + 1)) - counts.keys())
        unexpected = sorted(message_id for message_id in counts if message_id > self.expected_messages)
        return {
            "csv_file": str(self.csv_file),
            "qos": self.qos,
            "run": self.run,
            "expected_messages": self.expected_messages,
            "received_rows": len(self.message_ids),
            "unique_ids": len(counts),
            "missing_count": len(missing),
            "missing_ids": missing,
            "loss_pct": 100 * len(missing) / self.expected_messages,
            "duplicates": len(self.message_ids) - len(counts),
            "duplicate_ids": sorted(message_id for message_id, count in counts.items() if count > 1),
            "unexpected_count": len(unexpected),
            "unexpected_ids": unexpected,
            **latency_statistics(self.latencies_ms),
        }


def analyze_csv(path: Path, *, expected_messages: int = DEFAULT_EXPECTED_MESSAGES) -> RunAnalysis:
    if type(expected_messages) is not int or expected_messages < 1:
        raise ValueError("expected_messages must be a positive integer")
    path = Path(path).resolve()
    qos, run = detect_run(path)
    message_ids, latencies = [], []
    try:
        with path.open(encoding="utf-8-sig", newline="") as stream:
            reader = csv.DictReader(stream, strict=True)
            if not {"message_id", "latency_ms"}.issubset(reader.fieldnames or []):
                raise ValueError(f"{path.name}: required columns: message_id, latency_ms")
            for row in reader:
                try:
                    if None in row:
                        raise ValueError("too many CSV fields")
                    message_id = int(row["message_id"])
                    latency = float(row["latency_ms"])
                    if message_id < 1:
                        raise ValueError("message_id must be positive")
                    if not math.isfinite(latency):
                        raise ValueError("latency_ms must be finite")
                except (TypeError, ValueError) as exc:
                    raise ValueError(f"{path.name}, CSV line {reader.line_num}: {exc}") from exc
                message_ids.append(message_id)
                latencies.append(latency)
    except (csv.Error, UnicodeError) as exc:
        raise ValueError(f"{path.name}: cannot read CSV: {exc}") from exc
    return RunAnalysis(path, qos, run, expected_messages, tuple(message_ids), tuple(latencies))


def analyze_files(
    paths: Sequence[Path], *, expected_messages: int = DEFAULT_EXPECTED_MESSAGES,
) -> list[RunAnalysis]:
    if not paths:
        raise ValueError("At least one CSV is required")
    runs = [analyze_csv(path, expected_messages=expected_messages) for path in paths]
    identities = [(run.qos, run.run) for run in runs]
    if len(set(identities)) != len(identities):
        raise ValueError("Repeated (QoS, run) pair: provide only one CSV for each run")
    return sorted(runs, key=lambda run: (run.qos, run.run))


def aggregate_by_qos(runs: Sequence[RunAnalysis]) -> list[dict]:
    """IDs are scoped to each run; empty runs contribute loss but no latency."""
    aggregates = []
    for qos in sorted({run.qos for run in runs}):
        group = [run for run in runs if run.qos == qos]
        summaries = [run.summary() for run in group]
        expected = sum(row["expected_messages"] for row in summaries)
        missing = sum(row["missing_count"] for row in summaries)
        run_means = [row["latency_mean_ms"] for row in summaries if row["latency_mean_ms"] is not None]
        aggregates.append(
            {
                "qos": qos,
                "run_count": len(group),
                "runs_with_latency": len(run_means),
                "expected_messages": expected,
                "received_rows": sum(row["received_rows"] for row in summaries),
                "unique_ids": sum(row["unique_ids"] for row in summaries),
                "missing_count": missing,
                "loss_pct": 100 * missing / expected,
                "duplicates": sum(row["duplicates"] for row in summaries),
                "unexpected_count": sum(row["unexpected_count"] for row in summaries),
                **latency_statistics(value for run in group for value in run.latencies_ms),
                "latency_mean_of_run_means_ms": fmean(run_means) if run_means else None,
            }
        )
    return aggregates


def write_summaries(
    runs: Sequence[RunAnalysis], output_dir: Path = DEFAULT_OUTPUT_DIR,
) -> tuple[Path, Path]:
    if not runs:
        raise ValueError("No runs to summarize")
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S_%fZ") + "_" + uuid4().hex
    run_path = output_dir / f"qos_runs_{stamp}.csv"
    aggregate_path = output_dir / f"qos_aggregate_{stamp}.csv"
    for path, fields, rows in (
        (run_path, RUN_FIELDS, [run.summary() for run in runs]),
        (aggregate_path, AGGREGATE_FIELDS, aggregate_by_qos(runs)),
    ):
        with path.open("x", encoding="utf-8", newline="") as stream:
            writer = csv.DictWriter(stream, fieldnames=fields)
            writer.writeheader()
            for row in rows:
                writer.writerow(
                    {key: json.dumps(value) if isinstance(value, list) else value for key, value in row.items()}
                )
    return run_path, aggregate_path
