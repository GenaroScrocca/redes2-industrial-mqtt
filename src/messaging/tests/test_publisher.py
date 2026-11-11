import json
from types import SimpleNamespace

import paho.mqtt.client as mqtt
import pytest

from src.messaging import publisher
from src.messaging.client import MqttError
from src.messaging.config import MqttConfig
from src.simulator import PumpSimulator


def test_publisher_preserves_simulator_ids_seed_and_adds_transport_time(fake_client, monkeypatch):
    waits = []
    monkeypatch.setattr(publisher.time, "sleep", waits.append)
    monkeypatch.setattr(publisher.time, "time_ns", lambda: 1_794_420_000_123_456_789)
    config = MqttConfig("broker", 1884, "custom/telemetry", 1)
    publisher.publish_samples(config, samples=3, interval=0.25, seed=42)

    reference = PumpSimulator(seed=42)
    for index, (topic, encoded, qos, retain) in enumerate(fake_client.publications, start=1):
        payload = json.loads(encoded)
        physical_sample = reference.next_sample()
        assert payload.pop("sent_at_unix_ns") == 1_794_420_000_123_456_789
        assert payload["message_id"] == index
        payload.pop("timestamp")
        physical_sample.pop("timestamp")
        assert payload == physical_sample
        assert (topic, qos, retain) == (config.topic, 1, False)
    assert waits == [0.25, 0.25]
    assert fake_client.publish_info.waits == [10.0, 10.0, 10.0]
    assert fake_client.disconnect_calls == fake_client.stop_calls == 1


def test_transport_time_is_added_after_sampling_without_mutating_sample(fake_client, monkeypatch):
    physical_sample = PumpSimulator().next_sample()
    calls = []

    def sample():
        calls.append("sample")
        return physical_sample

    def now():
        calls.append("time")
        return 123

    monkeypatch.setattr(publisher, "PumpSimulator", lambda seed: SimpleNamespace(next_sample=sample))
    monkeypatch.setattr(publisher.time, "time_ns", now)
    publisher.publish_samples(MqttConfig(), samples=1, interval=0, seed=None)
    assert calls == ["sample", "time"]
    assert "sent_at_unix_ns" not in physical_sample


@pytest.mark.parametrize("failure", ["return_code", "timeout"])
def test_publish_failures_close_the_connection(failure, fake_client):
    if failure == "return_code":
        fake_client.publish_info.rc = mqtt.MQTT_ERR_NO_CONN
    else:
        fake_client.publish_info.published = False
    with pytest.raises(MqttError):
        publisher.publish_samples(MqttConfig(), samples=1, interval=0, seed=None)
    assert fake_client.disconnect_calls == fake_client.stop_calls == 1


def test_publisher_cli_passes_connection_overrides(fake_client):
    assert publisher.main([
        "--samples", "1", "--interval", "0", "--seed", "9", "--host", "other",
        "--port", "1885", "--topic", "p101/data", "--qos", "2",
    ]) == 0
    assert fake_client.connections == [("other", 1885, 60)]
    assert fake_client.publications[0][0::2] == ("p101/data", 2)


def test_connection_failure_returns_nonzero_without_traceback(fake_client, capsys):
    fake_client.connect_error = ConnectionRefusedError("refused")
    assert publisher.main(["--samples", "1"]) == 1
    assert "Error MQTT publisher:" in capsys.readouterr().err


def test_publisher_ctrl_c_closes_cleanly(fake_client, monkeypatch, capsys):
    def interrupt(*args, **kwargs):
        raise KeyboardInterrupt

    monkeypatch.setattr(fake_client, "publish", interrupt)
    assert publisher.main(["--samples", "1"]) == 130
    assert fake_client.stop_calls == 1
    assert "Publisher detenido" in capsys.readouterr().out
