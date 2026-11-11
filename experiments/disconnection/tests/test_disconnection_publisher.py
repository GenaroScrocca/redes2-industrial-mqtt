import csv
import json
from types import SimpleNamespace

import paho.mqtt.client as mqtt
import pytest

from experiments.disconnection import publisher
from experiments.disconnection.client import ResilientClient
from src.messaging.config import MqttConfig
from src.simulator import PumpSimulator


def read_rows(path):
    with path.open(encoding="utf-8", newline="") as stream:
        return list(csv.DictReader(stream))


def test_sampling_continues_through_disconnect_and_recovers_with_sequential_ids(
    fake_paho, fake_clock, tmp_path,
):
    fake_clock.actions = {
        2: fake_paho.lose_connection, 3: lambda: fake_paho.retry(fail=True),
        4: lambda: fake_paho.retry(fail=True), 5: fake_paho.retry,
    }
    paths = (tmp_path / "publisher.csv", tmp_path / "publisher_events.csv")
    assert publisher.run_publisher(
        MqttConfig(qos=1), samples=8, interval=1, seed=42, retry_interval=1, drain_seconds=0, paths=paths,
    ) == 0
    rows = sorted(read_rows(paths[0]), key=lambda row: int(row["message_id"]))
    assert [int(row["message_id"]) for row in rows] == list(range(1, 9))
    assert [int(row["message_id"]) for row in rows if row["publish_attempted"] == "False"] == [3, 4, 5]
    assert all(row["sent_at_unix_ns"] == "" for row in rows[2:5])
    assert all(row["publish_success"] == "False" for row in rows[2:5])
    assert all(row["publish_success"] == "True" for row in [*rows[:2], *rows[5:]])
    payloads = [json.loads(entry[1]) for entry in fake_paho.publications]
    assert [row["message_id"] for row in payloads] == [1, 2, 6, 7, 8]
    reference = PumpSimulator(seed=42)
    physical = {sample["message_id"]: sample for sample in [reference.next_sample() for _ in range(8)]}
    for payload in payloads:
        expected = physical[payload["message_id"]]
        assert payload.pop("sent_at_unix_ns") > 0
        payload.pop("timestamp")
        expected.pop("timestamp")
        assert payload == expected
    assert "RECONNECTED" in [row["event"] for row in read_rows(paths[1])]
    assert fake_paho.stops == fake_paho.disconnects == 1


def test_no_ack_is_never_inferred_as_success_from_publish_return_code(fake_paho, fake_clock, tmp_path):
    fake_paho.auto_ack = False
    paths = (tmp_path / "publisher.csv", tmp_path / "events.csv")
    publisher.run_publisher(
        MqttConfig(qos=1), samples=3, interval=0.1, seed=42,
        retry_interval=1, drain_seconds=0.05, paths=paths,
    )
    rows = read_rows(paths[0])
    assert len(rows) == 3
    assert all(row["publish_return_code"] == "0" for row in rows)
    assert all(row["publish_success"] == "False" and row["publish_status"] == "unconfirmed" for row in rows)


def test_offline_start_still_generates_all_samples_without_transport_timestamps(fake_paho, fake_clock, tmp_path):
    fake_paho.auto_connect = False
    paths = (tmp_path / "publisher.csv", tmp_path / "events.csv")
    publisher.run_publisher(
        MqttConfig(), samples=3, interval=0.1, seed=42, retry_interval=1, drain_seconds=0, paths=paths,
    )
    assert fake_paho.publications == []
    assert len(read_rows(paths[0])) == 3
    assert all(row["sent_at_unix_ns"] == "" for row in read_rows(paths[0]))


def test_inflight_qos1_can_complete_after_reconnection_without_mutating_sample(fake_paho):
    fake_paho.auto_ack = False
    fake_paho.publish_rc = mqtt.MQTT_ERR_NO_CONN
    session = ResilientClient(MqttConfig(qos=1), role="publisher")
    session.start()
    rows = []
    log = publisher.PublicationLog(session, SimpleNamespace(write=lambda row: rows.append(dict(row))))
    sample = PumpSimulator().next_sample()
    log.submit(sample)
    assert rows == []
    assert "sent_at_unix_ns" not in sample
    fake_paho.lose_connection()
    fake_paho.retry()
    fake_paho.on_publish(fake_paho, None, 1, SimpleNamespace(is_failure=False), None)
    log.collect()
    assert rows[0]["publish_success"] is True
    assert rows[0]["message_id"] == 1


@pytest.mark.parametrize("failure", ["no_connection", "exception", "rejected"])
def test_publication_failures_are_recorded(fake_paho, monkeypatch, failure):
    session = ResilientClient(MqttConfig(), role="publisher")
    session.start()
    rows = []
    log = publisher.PublicationLog(session, SimpleNamespace(write=lambda row: rows.append(dict(row))))
    if failure == "no_connection":
        fake_paho.publish_rc = mqtt.MQTT_ERR_NO_CONN
    elif failure == "exception":
        def fail(*args, **kwargs):
            raise ConnectionResetError("socket closed")
        monkeypatch.setattr(fake_paho, "publish", fail)
    else:
        fake_paho.auto_ack = False
    log.submit(PumpSimulator().next_sample())
    if failure == "rejected":
        fake_paho.on_publish(fake_paho, None, 1, SimpleNamespace(is_failure=True), None)
        log.collect()
    assert len(rows) == 1
    assert rows[0]["publish_attempted"] is True
    assert rows[0]["publish_success"] is False
    assert rows[0]["sent_at_unix_ns"] > 0


def test_publisher_cli_overrides_and_refuses_overwrite(fake_paho, fake_clock, tmp_path, capsys):
    args = ["--samples", "1", "--interval", "0", "--run-id", "test", "--output-dir", str(tmp_path),
            "--host", "other", "--port", "1884", "--topic", "test/disconnection", "--qos", "1"]
    assert publisher.main(args) == 0
    assert fake_paho.connection[:2] == ("other", 1884)
    assert fake_paho.publications[0][0] == "test/disconnection"
    assert publisher.main(args) == 1
    assert "Error publisher experimental" in capsys.readouterr().err


def test_ctrl_c_finalizes_pending_rows_and_closes(fake_paho, fake_clock, tmp_path):
    fake_paho.auto_ack = False

    def interrupt():
        raise KeyboardInterrupt

    fake_clock.actions = {1: interrupt}
    paths = (tmp_path / "publisher.csv", tmp_path / "events.csv")
    assert publisher.run_publisher(
        MqttConfig(qos=1), samples=3, interval=1, seed=42, retry_interval=1, drain_seconds=0, paths=paths,
    ) == 130
    rows = read_rows(paths[0])
    assert len(rows) == 1
    assert rows[0]["publish_status"] == "unconfirmed"
    assert fake_paho.stops == 1


def test_ctrl_c_during_publish_keeps_the_generated_sample(fake_paho, monkeypatch, tmp_path):
    def interrupt(*args, **kwargs):
        raise KeyboardInterrupt

    monkeypatch.setattr(fake_paho, "publish", interrupt)
    paths = (tmp_path / "publisher.csv", tmp_path / "events.csv")
    assert publisher.run_publisher(
        MqttConfig(), samples=3, interval=1, seed=42, retry_interval=1, drain_seconds=0, paths=paths,
    ) == 130
    rows = read_rows(paths[0])
    assert len(rows) == 1
    assert rows[0]["publish_attempted"] == "True"
    assert rows[0]["publish_status"] == "unconfirmed"
    assert fake_paho.stops == 1


def test_pending_rows_are_finalized_even_if_disconnect_raises(fake_paho, monkeypatch, tmp_path):
    fake_paho.auto_ack = False

    def fail():
        raise ConnectionResetError("socket already closed")

    monkeypatch.setattr(fake_paho, "disconnect", fail)
    paths = (tmp_path / "publisher.csv", tmp_path / "events.csv")
    with pytest.raises(ConnectionResetError):
        publisher.run_publisher(
            MqttConfig(qos=1), samples=1, interval=0, seed=42, retry_interval=1, drain_seconds=0, paths=paths,
        )
    assert read_rows(paths[0])[0]["publish_status"] == "unconfirmed"
    assert fake_paho.stops == 1
