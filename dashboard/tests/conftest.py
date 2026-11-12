import json
import time
from types import SimpleNamespace

import paho.mqtt.client as mqtt
import pytest

from dashboard.runtime import DemoRuntime
from src.messaging import config


class FakeClient:
    """In-memory MQTT boundary; forwarding is opt-in and exists only in tests."""

    def __init__(self, factory, role):
        self.factory, self.role = factory, role
        self.publications = []
        self.subscriptions = []
        self.stopped = self.disconnected = False

    def reconnect_delay_set(self, **kwargs):
        self.retry_delay = kwargs

    def max_queued_messages_set(self, size):
        self.queue_limit = size

    def connect_async(self, host, port, keepalive):
        self.connection = (host, port, keepalive)

    def connect(self):
        self.on_connect(self, None, None, SimpleNamespace(is_failure=False), None)

    def loop_start(self):
        if self.factory.online:
            self.connect()
        else:
            self.on_connect_fail(self, None)
        return mqtt.MQTT_ERR_SUCCESS

    def subscribe(self, topic, qos):
        self.subscriptions.append((topic, qos))
        if self.factory.ack_subscribe:
            self.ack_subscription()
        return mqtt.MQTT_ERR_SUCCESS, 1

    def ack_subscription(self, *, failure=False):
        self.on_subscribe(self, None, 1, [SimpleNamespace(is_failure=failure)], None)

    def publish(self, topic, payload, qos, retain):
        self.publications.append(json.loads(payload))
        if self.factory.deliver:
            self.factory.clients["subscriber"].receive(payload.encode())
        return SimpleNamespace(rc=mqtt.MQTT_ERR_SUCCESS)

    def receive(self, payload, *, retained=False):
        self.on_message(self, None, SimpleNamespace(payload=payload, retain=retained))

    def lose_connection(self):
        self.on_disconnect(self, None, None, "lost", None)

    def disconnect(self):
        self.disconnected = True
        self.on_disconnect(self, None, None, "intentional close", None)

    def loop_stop(self):
        self.stopped = True


class FakeBroker:
    online = True
    deliver = False
    ack_subscribe = True

    def __init__(self):
        self.clients = {}
        self.created = []

    def __call__(self, *args, **kwargs):
        role = "subscriber" if "subscriber" in kwargs["client_id"] else "publisher"
        client = FakeClient(self, role)
        self.clients[role] = client
        self.created.append(client)
        return client


@pytest.fixture
def fake_broker():
    return FakeBroker()


@pytest.fixture
def demo(fake_broker):
    runtime = DemoRuntime(client_factory=fake_broker)
    yield runtime
    runtime.stop()
    assert runtime.wait_stopped()


@pytest.fixture
def wait_for():
    def wait(predicate, timeout=3):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if predicate():
                return
            time.sleep(0.01)
        assert predicate(), "Background worker did not reach expected state"

    return wait


@pytest.fixture
def row():
    return {
        "asset_id": "P101", "message_id": 1, "timestamp": "2026-11-12T12:00:00+00:00",
        "received_at": "2026-11-12T12:00:00.002+00:00", "sent_at_unix_ns": 1,
        "pressure_bar": 5.2, "flow_lpm": 120.0, "temperature_c": 60.0,
        "vibration_mms": 2.0, "rpm": 1450, "latency_ms": 2.0,
    }


@pytest.fixture(autouse=True)
def clean_mqtt_environment(monkeypatch):
    for name in ("MQTT_BROKER_HOST", "MQTT_BROKER_PORT", "MQTT_TOPIC_BASE", "MQTT_QOS"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setattr(config, "load_dotenv", lambda *args, **kwargs: None)
