import csv
import json
from queue import Empty, Queue

from src.messaging import subscriber
from src.messaging.config import MqttConfig
from src.messaging.recording import CsvRecorder
from src.simulator import PumpSimulator

RECEIVED_NS = 1_794_420_000_123_456_789


def messages():
    simulator = PumpSimulator(seed=42)
    first = {**simulator.next_sample(), "sent_at_unix_ns": RECEIVED_NS - 2_000_000}
    second = {**simulator.next_sample(), "sent_at_unix_ns": RECEIVED_NS - 4_000_000}
    return [json.dumps(first).encode(), json.dumps(first).encode(), b"invalid", json.dumps(second).encode()]


def test_subscriber_records_duplicates_skips_invalid_and_stops_at_unique_target(
    fake_client, monkeypatch, tmp_path, capsys
):
    fake_client.messages = messages()
    monkeypatch.setattr(subscriber.time, "time_ns", lambda: RECEIVED_NS)
    monkeypatch.setattr(subscriber, "CsvRecorder", lambda: CsvRecorder(tmp_path))
    metrics = subscriber.receive_messages(MqttConfig(), expected_messages=2)

    assert metrics.total_received == 4
    assert metrics.unique_ids == {1, 2}
    assert metrics.duplicate_ids == {1}
    assert metrics.invalid_messages == 1
    assert metrics.latency_min == 2.0
    assert metrics.latency_max == 4.0
    assert metrics.latency_average == 8 / 3
    with next(tmp_path.glob("*.csv")).open(encoding="utf-8", newline="") as stream:
        rows = list(csv.DictReader(stream))
    assert [int(row["message_id"]) for row in rows] == [1, 1, 2]
    captured = capsys.readouterr()
    assert "Mensaje descartado:" in captured.err
    assert "asset_id=P101 pressure_bar=" in captured.out
    assert "latency_ms=2.000" in captured.out
    assert "Resumen MQTT" in captured.out
    assert fake_client.disconnect_calls == fake_client.stop_calls == 1


def test_ctrl_c_keeps_recorded_messages_and_prints_summary(fake_client, monkeypatch, tmp_path, capsys):
    class InterruptWhenEmpty(Queue):
        def get(self, *args, **kwargs):
            try:
                return Queue.get(self, block=False)
            except Empty:
                raise KeyboardInterrupt

    fake_client.messages = messages()[:2]
    monkeypatch.setattr(subscriber, "Queue", InterruptWhenEmpty)
    monkeypatch.setattr(subscriber.time, "time_ns", lambda: RECEIVED_NS)
    monkeypatch.setattr(subscriber, "CsvRecorder", lambda: CsvRecorder(tmp_path))
    metrics = subscriber.receive_messages(MqttConfig())
    assert metrics.total_received == 2
    assert fake_client.stop_calls == 1
    output = capsys.readouterr().out
    assert "Subscriber detenido" in output
    assert "Mensajes recibidos: 2" in output


def test_subscriber_cli_overrides_and_expected_messages(fake_client, monkeypatch, tmp_path):
    fake_client.messages = messages()[:1]
    monkeypatch.setattr(subscriber, "CsvRecorder", lambda: CsvRecorder(tmp_path))
    assert subscriber.main([
        "--expected-messages", "1", "--host", "broker", "--port", "1884",
        "--topic", "test/telemetry", "--qos", "1",
    ]) == 0
    assert fake_client.connections == [("broker", 1884, 60)]
    assert fake_client.subscriptions == [("test/telemetry", 1)]


def test_subscriber_connection_error_returns_nonzero(fake_client, capsys):
    fake_client.connect_error = ConnectionRefusedError("refused")
    assert subscriber.main(["--expected-messages", "1"]) == 1
    captured = capsys.readouterr()
    assert "Error MQTT subscriber:" in captured.err
    assert "Resumen MQTT" in captured.out
