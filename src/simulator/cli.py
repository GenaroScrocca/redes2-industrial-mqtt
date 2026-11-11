"""Command-line output of finite telemetry streams as JSON Lines."""

import argparse
import json
import math
import time
from collections.abc import Sequence

from .config import DEFAULT_INTERVAL_SECONDS, DEFAULT_SAMPLES
from .simulator import PumpSimulator


def _positive_int(value: str) -> int:
    try:
        number = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be a positive integer") from exc
    if number <= 0:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return number


def _interval_seconds(value: str) -> float:
    try:
        seconds = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be a finite number >= 0") from exc
    if not math.isfinite(seconds) or seconds < 0:
        raise argparse.ArgumentTypeError("must be a finite number >= 0")
    return seconds


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Generate P101 pump telemetry.")
    parser.add_argument(
        "-n", "--samples", type=_positive_int, default=DEFAULT_SAMPLES,
        help=f"number of samples (default: {DEFAULT_SAMPLES})",
    )
    parser.add_argument(
        "--interval", type=_interval_seconds, default=DEFAULT_INTERVAL_SECONDS,
        help=f"seconds between samples; 0 disables waiting (default: {DEFAULT_INTERVAL_SECONDS})",
    )
    parser.add_argument("--seed", type=int, help="optional seed for repeatable signal values")
    args = parser.parse_args(argv)

    simulator = PumpSimulator(seed=args.seed)
    try:
        for index in range(args.samples):
            if index > 0:
                time.sleep(args.interval)
            print(json.dumps(simulator.next_sample(), allow_nan=False), flush=True)
    except KeyboardInterrupt:
        return 130
    return 0
