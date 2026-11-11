import csv
import json
from types import SimpleNamespace

import pytest

from experiments.disconnection import subscriber
from experiments.disconnection.client import ResilientClient
from src.messaging.config import MqttConfig

BASE_NS = 1_700_000_000_000_000_000


def payload(message_id=1, sent=True):
    data = {
        "message_id": message_id, "asset_id": "P101", "timestamp": "2023-11-14T22:13:20+00:00",
        "pressure_bar": 5.2, "flow_lpm": 120, "temperature_c": 60, "vibration_mms": 2, "rpm": 1450,
    }
    if sent:
        data["sent_at_unix_ns"] = BASE_NS - 2_000_000
    return json.dumps(data).encode()


def test_decode_latency_is_optional_without_inventing_transport_time():
    row = subscriber.decode_received(payload(), BASE_NS)
    assert row["latency_ms"] == 2.0
    assert row["received_at_unix_ns"] == BASE_NS
    row = subscriber.decode_received(payload(sent=False), BASE_NS)
    assert row["latency_ms"] is None and row["sent_at_unix_ns"] is None


def test_reception_log_detects_duplicate_ids_and_discards_invalid_json(fake_paho):
    session = ResilientClient(MqttConfig(), role="subscriber")
    session.start()
    rows = []
    log = subscriber.ReceptionLog(SimpleNamespace(write=lambda row: rows.append(dict(row))))
    for encoded in (payload(), payload(), b"invalid", payload(3, sent=False)):
        session.messages.put((encoded, BASE_NS))
    log.collect(session)
    assert log.received == 3
    assert log.duplicates == log.invalid == 1
    assert log.ids == {1, 3}
    assert [row["is_duplicate"] for row in rows] == [False, True, False]
    assert rows[-1]["latency_ms"] is None


def test_subscriber_keeps_same_csv_after_recovery_and_drains_on_ctrl_c(fake_paho, monkeypatch, tmp_path):
    calls = 0

    def pause(_):
        nonlocal calls
        calls += 1
        if calls == 1:
            fake_paho.on_message(fake_paho, None, SimpleNamespace(payload=payload()))
            fake_paho.lose_connection()
        elif calls == 2:
            fake_paho.retry(fail=True)
            fake_paho.retry()
            fake_paho.on_message(fake_paho, None, SimpleNamespace(payload=payload(3)))
        else:
            raise KeyboardInterrupt

    monkeypatch.setattr(subscriber.time, "sleep", pause)
    paths = (tmp_path / "subscriber.csv", tmp_path / "events.csv")
    assert subscriber.run_subscriber(MqttConfig(), retry_interval=1, paths=paths) == 0
    with paths[0].open(encoding="utf-8", newline="") as stream:
        rows = list(csv.DictReader(stream))
    with paths[1].open(encoding="utf-8", newline="") as stream:
        events = list(csv.DictReader(stream))
    assert [row["message_id"] for row in rows] == ["1", "3"]
    assert {row["event"] for row in events} >= {"CONNECTED", "DISCONNECTED", "RECONNECT_ATTEMPT", "RECONNECTED"}
    assert len(fake_paho.subscriptions) == 2
    assert len(list(tmp_path.glob("*.csv"))) == 2
    assert fake_paho.stops == fake_paho.disconnects == 1


def test_subscriber_cli_reports_nontransient_subscription_error(fake_paho, tmp_path, capsys):
    fake_paho.subscribe_rejected = True
    assert subscriber.main(["--output-dir", str(tmp_path), "--run-id", "rejected"]) == 1
    assert "SUBACK rejected" in capsys.readouterr().err
    assert fake_paho.stops == 1


@pytest.mark.parametrize("encoded", [b"[]", b"{}", b"\xff", b"null"])
def test_malformed_payloads_are_not_recorded_as_telemetry(encoded):
    with pytest.raises((ValueError, UnicodeError)):
        subscriber.decode_received(encoded, BASE_NS)
