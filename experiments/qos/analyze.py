"""CLI: python -m experiments.qos.analyze <CSV files or glob patterns>."""

import argparse
import glob
import sys
from collections.abc import Sequence
from pathlib import Path

from .analysis import (
    DEFAULT_EXPECTED_MESSAGES,
    DEFAULT_OUTPUT_DIR,
    aggregate_by_qos,
    analyze_files,
    write_summaries,
)


def expand_inputs(patterns: Sequence[str]) -> list[Path]:
    """Expand wildcards in Python, including when launched from PowerShell."""
    paths = []
    seen = set()
    for pattern in patterns:
        exact = Path(pattern)
        matches = [exact] if exact.is_file() else [Path(match) for match in sorted(glob.glob(pattern))]
        if not matches:
            raise ValueError(f"No CSV matches: {pattern}")
        for path in matches:
            if not path.is_file():
                raise ValueError(f"Not a file: {path}")
            resolved = path.resolve()
            if resolved not in seen:
                paths.append(resolved)
                seen.add(resolved)
    return paths


def format_comparison(aggregates: Sequence[dict]) -> str:
    groups = {row["qos"]: row for row in aggregates}
    metrics = (
        ("Corridas", "run_count"),
        ("Corridas con latencia", "runs_with_latency"),
        ("Mensajes esperados", "expected_messages"),
        ("Recibidos (filas)", "received_rows"),
        ("IDs unicos (suma por corrida)", "unique_ids"),
        ("Mensajes faltantes", "missing_count"),
        ("Perdida (%)", "loss_pct"),
        ("Duplicados (filas adicionales)", "duplicates"),
        ("IDs fuera de rango (por corrida)", "unexpected_count"),
        ("Latencia media global (ms)", "latency_mean_ms"),
        ("Promedio de medias por corrida (ms)", "latency_mean_of_run_means_ms"),
        ("Mediana global (ms)", "latency_median_ms"),
        ("P95 global (ms)", "latency_p95_ms"),
        ("Minima (ms)", "latency_min_ms"),
        ("Maxima (ms)", "latency_max_ms"),
    )

    def cell(qos: int, field: str) -> str:
        if qos not in groups:
            return "sin datos"
        value = groups[qos][field]
        if value is None:
            return "N/A"
        return f"{value:.6f}" if isinstance(value, float) else str(value)

    rows = [(label, cell(0, field), cell(1, field)) for label, field in metrics]
    widths = [max(len(row[column]) for row in [("Metrica", "QoS 0", "QoS 1"), *rows]) for column in range(3)]
    lines = ["Comparacion MQTT QoS 0 y QoS 1", f"{'Metrica':<{widths[0]}}  {'QoS 0':>{widths[1]}}  {'QoS 1':>{widths[2]}}"]
    lines.extend(f"{label:<{widths[0]}}  {qos0:>{widths[1]}}  {qos1:>{widths[2]}}" for label, qos0, qos1 in rows)
    lines.append("Latencias: todas las filas recibidas, incluidos duplicados; P95 con interpolacion lineal.")
    return "\n".join(lines)


def _positive_int(value: str) -> int:
    try:
        number = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be a positive integer") from exc
    if number < 1:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return number


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Analyze manually collected QoS 0 and QoS 1 CSV files.")
    parser.add_argument("csv_files", nargs="+", help="CSV paths or quoted glob patterns containing qosN_runN")
    parser.add_argument(
        "--expected-messages", type=_positive_int, default=DEFAULT_EXPECTED_MESSAGES,
        help=f"expected IDs per run: 1..N (default: {DEFAULT_EXPECTED_MESSAGES})",
    )
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR, help="summary CSV directory")
    args = parser.parse_args(argv)
    try:
        paths = expand_inputs(args.csv_files)
        runs = analyze_files(paths, expected_messages=args.expected_messages)
        run_path, aggregate_path = write_summaries(runs, args.output_dir)
        print(format_comparison(aggregate_by_qos(runs)))
        print(f"\nResumen por corrida: {run_path}")
        print(f"Resumen por QoS: {aggregate_path}")
    except (OSError, ValueError) as exc:
        print(f"Error de analisis: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
