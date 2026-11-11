"""Receive telemetry, record CSV rows, and report basic run statistics."""

import argparse
import sys
import time
from collections.abc import Sequence
from queue import Empty, Queue

from .cli import add_mqtt_arguments, mqtt_config, positive_int
from .client import MqttError, MqttSession
from .config import MqttConfig
from .metrics import TelemetryMetrics
from .recording import CsvRecorder
from .telemetry import decode_telemetry


def receive_messages(config: MqttConfig, *, expected_messages: int | None = None) -> TelemetryMetrics:
    metrics = TelemetryMetrics()
    inbox: Queue[tuple[bytes, int]] = Queue()

    def on_message(client, userdata, message) -> None:
        received_ns = time.time_ns()
        inbox.put((message.payload, received_ns))

    try:
        with MqttSession(config) as session:
            session.client.on_message = on_message
            with CsvRecorder() as recorder:
                session.subscribe()
                print(f"Suscripto a {config.topic}; QoS={config.qos}; CSV={recorder.path}", flush=True)
                while expected_messages is None or len(metrics.unique_ids) < expected_messages:
                    session.check_connection()
                    try:
                        payload, received_ns = inbox.get(timeout=0.2)
                    except Empty:
                        continue
                    try:
                        row = decode_telemetry(payload, received_ns)
                    except ValueError as exc:
                        metrics.record_invalid()
                        print(f"Mensaje descartado: {exc}", file=sys.stderr)
                        continue
                    recorder.write(row)
                    metrics.record(row)
                    print(
                        f"message_id={row['message_id']} asset_id={row['asset_id']} "
                        f"pressure_bar={row['pressure_bar']} latency_ms={row['latency_ms']:.3f}",
                        flush=True,
                    )
    except KeyboardInterrupt:
        print("\nSubscriber detenido.")
    finally:
        print(metrics.summary(), flush=True)
    return metrics


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Receive P101 telemetry and save results/raw CSV.")
    add_mqtt_arguments(parser)
    parser.add_argument("--expected-messages", type=positive_int, help="stop after N unique IDs")
    args = parser.parse_args(argv)
    config = mqtt_config(parser, args)
    try:
        receive_messages(config, expected_messages=args.expected_messages)
    except (MqttError, OSError, RuntimeError, ValueError) as exc:
        print(f"Error MQTT subscriber: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
