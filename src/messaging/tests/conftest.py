from types import SimpleNamespace

import paho.mqtt.client as mqtt
import pytest

from src.messaging import client, config


class FakePublishInfo:
    def __init__(self):
        self.rc = mqtt.MQTT_ERR_SUCCESS
        self.published = True
        self.waits = []

    def wait_for_publish(self, timeout=None):
        self.waits.append(timeout)

    def is_published(self):
        return self.published


class FakeClient:
    """Synchronous Paho boundary double: never opens sockets or starts threads."""

    def __init__(self):
        self.on_message = None
        self.connect_reason = SimpleNamespace(is_failure=False)
        self.subscribe_reasons = [SimpleNamespace(is_failure=False)]
        self.connect_error = None
        self.connect_rc = mqtt.MQTT_ERR_SUCCESS
        self.subscribe_rc = mqtt.MQTT_ERR_SUCCESS
        self.ack_connect = True
        self.ack_subscribe = True
        self.messages = []
        self.connections = []
        self.subscriptions = []
        self.publications = []
        self.publish_info = FakePublishInfo()
        self.disconnect_calls = 0
        self.stop_calls = 0

    def connect(self, host, port, keepalive):
        self.connections.append((host, port, keepalive))
        if self.connect_error:
            raise self.connect_error
        return self.connect_rc

    def loop_start(self):
        if self.ack_connect:
            self.on_connect(self, None, None, self.connect_reason, None)
        return mqtt.MQTT_ERR_SUCCESS

    def subscribe(self, topic, qos):
        self.subscriptions.append((topic, qos))
        if self.ack_subscribe:
            self.on_subscribe(self, None, 1, self.subscribe_reasons, None)
        if self.subscribe_rc == mqtt.MQTT_ERR_SUCCESS:
            for payload in self.messages:
                self.on_message(self, None, SimpleNamespace(payload=payload))
        return self.subscribe_rc, 1

    def publish(self, topic, payload, qos, retain):
        self.publications.append((topic, payload, qos, retain))
        return self.publish_info

    def disconnect(self):
        self.disconnect_calls += 1
        self.on_disconnect(self, None, None, SimpleNamespace(is_failure=False), None)

    def loop_stop(self):
        self.stop_calls += 1


@pytest.fixture(autouse=True)
def isolated_mqtt_environment(monkeypatch):
    for name in ("MQTT_BROKER_HOST", "MQTT_BROKER_PORT", "MQTT_TOPIC_BASE", "MQTT_QOS"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setattr(config, "load_dotenv", lambda *args, **kwargs: None)


@pytest.fixture
def fake_client(monkeypatch):
    fake = FakeClient()

    def factory(*args, **kwargs):
        fake.constructor_args = args
        fake.constructor_kwargs = kwargs
        return fake

    monkeypatch.setattr(client.mqtt, "Client", factory)
    return fake
