"""Shared broker settings, loaded from .env and the process environment."""

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW_RESULTS_DIR = PROJECT_ROOT / "results" / "raw"
MQTT_TIMEOUT_SECONDS = 10.0


@dataclass(frozen=True)
class MqttConfig:
    host: str = "localhost"
    port: int = 1883
    topic: str = "industrial/station01/pump/P101/telemetry"
    qos: int = 0

    def __post_init__(self) -> None:
        if not self.host.strip():
            raise ValueError("MQTT host must not be empty")
        if not 1 <= self.port <= 65535:
            raise ValueError("MQTT port must be between 1 and 65535")
        if self.qos not in (0, 1, 2):
            raise ValueError("MQTT QoS must be 0, 1 or 2")
        if (
            not self.topic
            or any(character in self.topic for character in ("+", "#", "\x00"))
            or len(self.topic.encode("utf-8")) > 65535
        ):
            raise ValueError("MQTT topic must be a nonempty exact topic without wildcards")

    @classmethod
    def from_env(cls, *, host=None, port=None, topic=None, qos=None) -> "MqttConfig":
        """Precedence: explicit CLI values > environment > .env > defaults."""
        load_dotenv(PROJECT_ROOT / ".env", override=False)
        base = os.getenv("MQTT_TOPIC_BASE", "industrial/station01/pump/P101").rstrip("/")
        if topic is None and not base:
            raise ValueError("MQTT_TOPIC_BASE must not be empty")
        return cls(
            host=host if host is not None else os.getenv("MQTT_BROKER_HOST", "localhost"),
            port=int(port if port is not None else os.getenv("MQTT_BROKER_PORT", "1883")),
            topic=topic if topic is not None else f"{base}/telemetry",
            qos=int(qos if qos is not None else os.getenv("MQTT_QOS", "0")),
        )
