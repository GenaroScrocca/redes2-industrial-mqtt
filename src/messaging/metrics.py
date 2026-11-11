"""In-memory counters for a single subscriber execution."""

from dataclasses import dataclass, field


@dataclass
class TelemetryMetrics:
    total_received: int = 0
    invalid_messages: int = 0
    unique_ids: set[int] = field(default_factory=set)
    duplicate_ids: set[int] = field(default_factory=set)
    duplicate_count: int = 0
    latency_min: float | None = None
    latency_max: float | None = None
    _latency_sum: float = 0.0

    def record(self, row: dict) -> None:
        self.total_received += 1
        message_id, latency = row["message_id"], row["latency_ms"]
        if message_id in self.unique_ids:
            self.duplicate_ids.add(message_id)
            self.duplicate_count += 1
        self.unique_ids.add(message_id)
        self.latency_min = latency if self.latency_min is None else min(self.latency_min, latency)
        self.latency_max = latency if self.latency_max is None else max(self.latency_max, latency)
        self._latency_sum += latency

    def record_invalid(self) -> None:
        self.total_received += 1
        self.invalid_messages += 1

    @property
    def latency_average(self) -> float | None:
        count = self.total_received - self.invalid_messages
        return self._latency_sum / count if count else None

    def summary(self) -> str:
        def milliseconds(value: float | None) -> str:
            return "N/A" if value is None else f"{value:.3f}"

        return "\n".join(
            [
                "Resumen MQTT",
                f"Mensajes recibidos: {self.total_received}",
                f"Mensajes invalidos: {self.invalid_messages}",
                f"IDs unicos: {len(self.unique_ids)}",
                f"Mensajes duplicados: {self.duplicate_count}",
                f"IDs duplicados: {sorted(self.duplicate_ids)}",
                f"Latencia minima (ms): {milliseconds(self.latency_min)}",
                f"Latencia maxima (ms): {milliseconds(self.latency_max)}",
                f"Latencia promedio (ms): {milliseconds(self.latency_average)}",
            ]
        )
