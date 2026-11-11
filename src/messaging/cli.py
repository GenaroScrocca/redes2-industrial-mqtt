"""Common command-line arguments and validation."""

import argparse
import math

from .config import MqttConfig


def positive_int(value: str) -> int:
    try:
        number = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be a positive integer") from exc
    if number < 1:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return number


def interval_seconds(value: str) -> float:
    try:
        seconds = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be a finite number >= 0") from exc
    if not math.isfinite(seconds) or seconds < 0:
        raise argparse.ArgumentTypeError("must be a finite number >= 0")
    return seconds


def add_mqtt_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--host", help="broker host (MQTT_BROKER_HOST)")
    parser.add_argument("--port", type=int, help="broker port (MQTT_BROKER_PORT)")
    parser.add_argument("--topic", help="full telemetry topic (default: MQTT_TOPIC_BASE/telemetry)")
    parser.add_argument("--qos", type=int, choices=(0, 1, 2), help="MQTT QoS (MQTT_QOS)")


def mqtt_config(parser: argparse.ArgumentParser, args: argparse.Namespace) -> MqttConfig:
    try:
        return MqttConfig.from_env(host=args.host, port=args.port, topic=args.topic, qos=args.qos)
    except ValueError as exc:
        parser.error(str(exc))
