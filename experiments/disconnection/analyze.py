"""Analyze CSVs from a manually interrupted broker experiment."""

import argparse
import sys
from pathlib import Path

from experiments.qos.analysis import DEFAULT_OUTPUT_DIR

from .analysis import analyze_run, write_summary


def format_summary(summary: dict) -> str:
    labels = (
        ("Muestras generadas", "generated_samples"),
        ("Publicaciones con finalizacion observada", "publications_successful"),
        ("Fallidas/sin exito confirmado", "publications_unsuccessful"),
        ("Sin intento (broker no disponible)", "publications_not_attempted"),
        ("Intentadas sin confirmacion al cierre", "publications_unconfirmed"),
        ("Mensajes recibidos", "received_messages"),
        ("IDs faltantes", "missing_count"), ("Perdida sobre generadas (%)", "loss_pct"),
        ("Duplicados", "duplicates"), ("Interrupciones observadas", "interruption_count"),
        ("Desconexion (primera)", "disconnected_at"), ("Reconexion (primera)", "reconnected_at"),
        ("Duracion de interrupcion (s)", "interruption_duration_s"),
        ("Hasta primer mensaje tras reconexion (s)", "recovery_to_first_message_s"),
        ("Muestras con latencia", "latency_samples"), ("Latencia minima (ms)", "latency_min_ms"),
        ("Latencia maxima (ms)", "latency_max_ms"), ("Latencia promedio (ms)", "latency_mean_ms"),
        ("Mediana (ms)", "latency_median_ms"), ("P95 (ms)", "latency_p95_ms"),
    )
    lines = ["Resumen del experimento de desconexion"]
    for label, key in labels:
        value = summary[key]
        display = "N/A" if value is None else f"{value:.6f}" if isinstance(value, float) else str(value)
        lines.append(f"{label}: {display}")
    lines.append("Los tiempos describen detecciones del subscriber. Sin datos se informa N/A.")
    return "\n".join(lines)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Analyze publisher, subscriber and subscriber event CSVs.")
    parser.add_argument("--publisher", type=Path, required=True)
    parser.add_argument("--subscriber", type=Path, required=True)
    parser.add_argument("--events", type=Path, required=True, help="subscriber events CSV")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    args = parser.parse_args(argv)
    try:
        summary = analyze_run(args.publisher, args.subscriber, args.events)
        output = write_summary(summary, args.output_dir)
        print(format_summary(summary))
        print(f"Resumen CSV={output}")
    except (OSError, ValueError) as exc:
        print(f"Error de analisis: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
