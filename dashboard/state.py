"""Bounded, locked state updated exclusively from validated MQTT receptions."""

import math
import time
from collections import OrderedDict, deque
from dataclasses import dataclass, field
from threading import RLock

from src.messaging.config import MqttConfig


@dataclass(frozen=True)
class DemoConfig:
    mqtt: MqttConfig = field(default_factory=lambda: MqttConfig(qos=1))
    interval: float = 0.5
    seed: int = 42

    def __post_init__(self):
        if self.mqtt.qos not in (0, 1):
            raise ValueError("La demo admite QoS 0 o 1.")
        if not math.isfinite(self.interval) or not 0.1 <= self.interval <= 10:
            raise ValueError("El intervalo debe estar entre 0.1 y 10 segundos.")


@dataclass(frozen=True)
class Snapshot:
    token: str
    config: DemoConfig
    active: bool
    broker: str
    status: str
    samples: tuple[dict, ...]
    received: int
    duplicates: int
    rejected: int
    rate: float
    latency_mean: float | None
    latency_p95: float | None
    error: str
    buffer_size: int

    @property
    def latest(self) -> dict | None:
        return self.samples[-1] if self.samples else None


class DemoState:
    def __init__(self, buffer_size: int = 200):
        if buffer_size < 2:
            raise ValueError("buffer_size must be at least 2")
        self._lock = RLock()
        self.buffer_size = buffer_size
        self._samples = deque(maxlen=buffer_size)
        self._arrivals = deque(maxlen=buffer_size)
        self._seen = OrderedDict()
        self._config = DemoConfig()
        self._token = ""
        self._clear()

    def _clear(self):
        self._samples.clear()
        self._arrivals.clear()
        self._seen.clear()
        self._active = False
        self._links = {"publisher": False, "subscriber": False}
        self._subscribed = False
        self._failed = False
        self._received = self._duplicates = self._rejected = 0
        self._latency_mean = None
        self._error = ""
        self._started = time.monotonic()

    def begin(self, token: str, config: DemoConfig):
        with self._lock:
            self._clear()
            self._token, self._config, self._active = token, config, True

    def accepts(self, token: str) -> bool:
        with self._lock:
            return self._active and token == self._token

    def link(self, token: str, role: str, online: bool, detail: str = ""):
        with self._lock:
            if not self.accepts(token):
                return
            self._links[role] = online
            if role == "subscriber" and not online:
                self._subscribed = False
            if not online:
                self._failed, self._error = True, detail
            elif all(self._links.values()) and self._subscribed:
                self._failed, self._error = False, ""

    def subscribed(self, token: str):
        with self._lock:
            if self.accepts(token):
                self._subscribed = True
                if all(self._links.values()):
                    self._failed, self._error = False, ""

    def reject(self, token: str):
        with self._lock:
            if self.accepts(token):
                self._rejected += 1

    def receive(self, token: str, row: dict, *, arrived_at: float | None = None) -> bool:
        """Caller supplies decoded MQTT data; no generated samples enter this method."""
        with self._lock:
            if not self.accepts(token):
                return False
            latency = row["latency_ms"]
            if not math.isfinite(latency):
                self._rejected += 1
                return False
            self._duplicates += int(row["message_id"] in self._seen)
            self._seen[row["message_id"]] = None
            self._seen.move_to_end(row["message_id"])
            if len(self._seen) > self.buffer_size:
                self._seen.popitem(last=False)
            self._received += 1
            self._latency_mean = latency if self._latency_mean is None else (
                self._latency_mean * ((self._received - 1) / self._received) + latency / self._received
            )
            self._samples.append(dict(row))
            self._arrivals.append(time.monotonic() if arrived_at is None else arrived_at)
            return True

    def stop(self, token: str):
        with self._lock:
            if token == self._token:
                self._active = False
                self._links = {"publisher": False, "subscriber": False}
                self._subscribed = self._failed = False

    def reset(self):
        with self._lock:
            self._token = ""  # Reject callbacks from a worker that is still closing.
            self._clear()

    def worker_finished(self, token: str):
        with self._lock:
            if token == self._token:
                self._active = False
                self._links = {"publisher": False, "subscriber": False}
                self._subscribed = False

    def snapshot(self, *, now: float | None = None) -> Snapshot:
        now = time.monotonic() if now is None else now
        with self._lock:
            ready = all(self._links.values()) and self._subscribed
            broker = "OFFLINE" if self._failed else "IDLE" if not self._active else "ONLINE" if ready else "CONNECTING"
            fresh = bool(self._arrivals) and now - self._arrivals[-1] <= max(2, self._config.interval * 4)
            status = "BROKER OFFLINE" if broker == "OFFLINE" else "OPERATING" if self._active and ready and fresh else "STOPPED"
            elapsed = min(5.0, max(0.0, now - self._started))
            rate = sum(now - 5 <= arrival <= now for arrival in self._arrivals) / elapsed if elapsed >= 1 and self._active else 0.0
            latencies = sorted(row["latency_ms"] for row in self._samples)
            p95 = None
            if len(latencies) >= 20:
                position = (len(latencies) - 1) * 0.95
                lower, upper = math.floor(position), math.ceil(position)
                weight = position - lower
                p95 = latencies[lower] * (1 - weight) + latencies[upper] * weight
            return Snapshot(
                self._token, self._config, self._active, broker, status,
                tuple(dict(row) for row in self._samples), self._received, self._duplicates,
                self._rejected, rate, self._latency_mean, p95, self._error, self.buffer_size,
            )
