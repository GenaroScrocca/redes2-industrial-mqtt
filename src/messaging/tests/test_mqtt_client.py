from types import SimpleNamespace

import paho.mqtt.client as mqtt
import pytest

from src.messaging import client
from src.messaging.client import MqttError, MqttSession
from src.messaging.config import MqttConfig


def test_client_uses_v2_callbacks_waits_for_subscription_and_closes(fake_client):
    with MqttSession(MqttConfig()) as session:
        session.subscribe()
        session.check_connection()
        assert fake_client.constructor_args == (mqtt.CallbackAPIVersion.VERSION2,)
        assert fake_client.constructor_kwargs == {"reconnect_on_failure": False}
        assert fake_client.connections == [("localhost", 1883, 60)]
        assert fake_client.subscriptions == [(MqttConfig().topic, 0)]
    assert fake_client.disconnect_calls == 1
    assert fake_client.stop_calls == 1


def test_connection_exception_is_clear_and_closes(fake_client):
    fake_client.connect_error = ConnectionRefusedError("connection refused")
    with pytest.raises(MqttError, match="localhost:1883"):
        with MqttSession(MqttConfig()):
            pytest.fail("connection should fail")
    assert fake_client.disconnect_calls == fake_client.stop_calls == 1


def test_rejected_connection_closes(fake_client):
    fake_client.connect_reason = SimpleNamespace(is_failure=True)
    with pytest.raises(MqttError, match="rejected connection"):
        with MqttSession(MqttConfig()):
            pass
    assert fake_client.stop_calls == 1


def test_connack_timeout_closes(fake_client, monkeypatch):
    fake_client.ack_connect = False
    monkeypatch.setattr(client, "MQTT_TIMEOUT_SECONDS", 0.001)
    with pytest.raises(MqttError, match="CONNACK"):
        with MqttSession(MqttConfig()):
            pass
    assert fake_client.stop_calls == 1


@pytest.mark.parametrize("failure", ["rejected", "timeout", "return_code"])
def test_subscription_failures_are_reported_and_closed(failure, fake_client, monkeypatch):
    monkeypatch.setattr(client, "MQTT_TIMEOUT_SECONDS", 0.001)
    if failure == "rejected":
        fake_client.subscribe_reasons = [SimpleNamespace(is_failure=True)]
    elif failure == "timeout":
        fake_client.ack_subscribe = False
    else:
        fake_client.subscribe_rc = mqtt.MQTT_ERR_NO_CONN
    with pytest.raises(MqttError):
        with MqttSession(MqttConfig()) as session:
            session.subscribe()
    assert fake_client.stop_calls == 1


def test_unexpected_disconnect_is_reported(fake_client):
    with MqttSession(MqttConfig()) as session:
        fake_client.on_disconnect(fake_client, None, None, "connection lost", None)
        with pytest.raises(MqttError, match="connection lost"):
            session.check_connection()
