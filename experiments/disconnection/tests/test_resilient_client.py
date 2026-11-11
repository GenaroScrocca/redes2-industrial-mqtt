from queue import Empty
from types import SimpleNamespace

import paho.mqtt.client as mqtt
import pytest

from experiments.disconnection.client import ResilientClient
from src.messaging.config import MqttConfig


def take_events(session):
    events = []
    while True:
        try:
            events.append(session.events.get_nowait())
        except Empty:
            return events


def test_async_start_uses_fixed_retry_delay_and_v2_callbacks(fake_paho):
    session = ResilientClient(MqttConfig(), role="publisher", retry_interval=2)
    session.start()
    assert fake_paho.constructor == (
        (mqtt.CallbackAPIVersion.VERSION2,), {"clean_session": True, "reconnect_on_failure": True},
    )
    assert fake_paho.retry_delays == {"min_delay": 2, "max_delay": 2}
    assert fake_paho.connection == ("localhost", 1883, 10)
    assert session.connected.is_set() and session.ready.is_set()
    assert [row["event"] for row in take_events(session)] == ["CONNECT_ATTEMPT", "CONNECTED"]
    session.stop()
    session.stop()
    assert fake_paho.stops == fake_paho.disconnects == 1
    assert take_events(session) == []  # Intentional shutdown is not an outage.


def test_disconnect_retries_and_recovery_generate_timestamped_transitions(fake_paho, fake_clock):
    session = ResilientClient(MqttConfig(), role="publisher")
    session.start()
    fake_clock.sleep(1)
    fake_paho.lose_connection()
    assert not session.connected.is_set()
    for _ in range(2):
        fake_clock.sleep(1)
        fake_paho.retry(fail=True)
    fake_clock.sleep(1)
    fake_paho.retry()
    assert session.connected.is_set()
    events = take_events(session)
    assert [row["event"] for row in events] == [
        "CONNECT_ATTEMPT", "CONNECTED", "DISCONNECTED", "RECONNECT_ATTEMPT",
        "RECONNECT_ATTEMPT", "RECONNECT_ATTEMPT", "RECONNECTED",
    ]
    assert all(row["timestamp"].endswith("+00:00") for row in events)
    assert events[-1]["unix_ns"] > events[2]["unix_ns"]


def test_subscriber_resubscribes_on_each_reconnection(fake_paho):
    config = MqttConfig(topic="experiment/telemetry", qos=1)
    session = ResilientClient(config, role="subscriber")
    session.start()
    assert session.ready.is_set()
    fake_paho.lose_connection()
    assert not session.ready.is_set()
    fake_paho.retry()
    assert session.ready.is_set()
    assert fake_paho.subscriptions == [(config.topic, 1), (config.topic, 1)]
    assert [row["event"] for row in take_events(session)].count("SUBSCRIBED") == 2


def test_initial_unavailability_retries_until_first_connection(fake_paho):
    fake_paho.auto_connect = False
    session = ResilientClient(MqttConfig(), role="publisher")
    session.start()
    assert not session.connected.is_set()
    fake_paho.retry(fail=True)
    fake_paho.retry()
    events = [row["event"] for row in take_events(session)]
    assert events == ["CONNECT_ATTEMPT", "DISCONNECTED", "RECONNECT_ATTEMPT", "RECONNECT_ATTEMPT", "CONNECTED"]


def test_rejected_connack_does_not_mark_client_connected(fake_paho):
    fake_paho.auto_connect = False
    session = ResilientClient(MqttConfig(), role="publisher")
    session.start()
    fake_paho.on_connect(fake_paho, None, None, SimpleNamespace(is_failure=True), None)
    assert not session.connected.is_set()


def test_rejected_subscription_is_reported(fake_paho):
    fake_paho.subscribe_rejected = True
    session = ResilientClient(MqttConfig(), role="subscriber")
    session.start()
    assert not session.ready.is_set()
    assert "SUBACK rejected" in session.error


def test_callbacks_during_shutdown_do_not_report_a_false_recovery(fake_paho):
    session = ResilientClient(MqttConfig(), role="subscriber")
    session.start()
    take_events(session)
    session.stop()
    fake_paho.retry()
    assert not session.connected.is_set()
    assert take_events(session) == []


@pytest.mark.parametrize("role,retry", [("unknown", 1), ("publisher", 0)])
def test_invalid_client_options_are_rejected(role, retry):
    with pytest.raises(ValueError):
        ResilientClient(MqttConfig(), role=role, retry_interval=retry)
