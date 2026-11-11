"""Publish PumpSimulator samples without changing the physical model."""

import argparse
import json
import sys
import time
from collections.abc import Sequence

import paho.mqtt.client as mqtt

from src.simulator import PumpSimulator
from src.simulator.config import DEFAULT_INTERVAL_SECONDS, DEFAULT_SAMPLES

from .cli import add_mqtt_arguments, interval_seconds, mqtt_config, positive_int
from .client import MqttError, MqttSession
from .config import MQTT_TIMEOUT_SECONDS, MqttConfig


def publish_samples(config: MqttConfig, *, samples: int, interval: float, seed: int | None) -> None:
    simulator = PumpSimulator(seed=seed)
    with MqttSession(config) as session:
        print(f"Conectado a {config.host}:{config.port}; topic={config.topic}; QoS={config.qos}", flush=True)
        for index in range(samples):
            if index:
                time.sleep(interval)
            session.check_connection()
            payload = {**simulator.next_sample(), "sent_at_unix_ns": time.time_ns()}
            info = session.client.publish(
                config.topic, json.dumps(payload, allow_nan=False), qos=config.qos, retain=False
            )
            if info.rc != mqtt.MQTT_ERR_SUCCESS:
                raise MqttError(f"Publish failed: {mqtt.error_string(info.rc)}")
            info.wait_for_publish(timeout=MQTT_TIMEOUT_SECONDS)
            if not info.is_published():
                raise MqttError(f"Timed out publishing message_id={payload['message_id']}")
            print(f"Publicado message_id={payload['message_id']} asset_id={payload['asset_id']}", flush=True)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Publish P101 telemetry to Mosquitto.")
    add_mqtt_arguments(parser)
    parser.add_argument("-n", "--samples", type=positive_int, default=DEFAULT_SAMPLES)
    parser.add_argument("--interval", type=interval_seconds, default=DEFAULT_INTERVAL_SECONDS)
    parser.add_argument("--seed", type=int, help="optional simulator seed")
    args = parser.parse_args(argv)
    config = mqtt_config(parser, args)
    try:
        publish_samples(config, samples=args.samples, interval=args.interval, seed=args.seed)
    except KeyboardInterrupt:
        print("\nPublisher detenido.")
        return 130
    except (MqttError, OSError, RuntimeError, ValueError) as exc:
        print(f"Error MQTT publisher: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
