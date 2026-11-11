from types import SimpleNamespace

import paho.mqtt.client as mqtt
import pytest

from experiments.disconnection import client
from src.messaging import config

BASE_NS = 1_700_000_000_000_000_000


class ImmediateEvent:
    def __init__(self):
        self.value = False

    def set(self):
        self.value = True

    def clear(self):
        self.value = False

    def is_set(self):
        return self.value

    def wait(self, timeout=None):
        return self.value


class FakePaho:
    """Drive callbacks explicitly; no sockets, network threads or broker service."""

    def __init__(self):
        self.auto_connect = True
        self.auto_ack = True
        self.publish_rc = mqtt.MQTT_ERR_SUCCESS
        self.loop_rc = mqtt.MQTT_ERR_SUCCESS
        self.subscribe_rejected = False
        self.publications = []
        self.subscriptions = []
        self.stops = self.disconnects = self.mid = 0

    def reconnect_delay_set(self, **kwargs):
        self.retry_delays = kwargs

    def connect_async(self, host, port, keepalive):
        self.connection = (host, port, keepalive)

    def loop_start(self):
        self.on_pre_connect(self, None)
        if self.auto_connect:
            self.on_connect(self, None, None, SimpleNamespace(is_failure=False), None)
        else:
            self.on_connect_fail(self, None)
        return self.loop_rc

    def lose_connection(self):
        self.on_disconnect(self, None, None, "broker unavailable", None)

    def retry(self, *, fail=False):
        self.on_pre_connect(self, None)
        if fail:
            self.on_connect_fail(self, None)
        else:
            self.on_connect(self, None, None, SimpleNamespace(is_failure=False), None)

    def subscribe(self, topic, qos):
        self.subscriptions.append((topic, qos))
        self.on_subscribe(self, None, 1, [SimpleNamespace(is_failure=self.subscribe_rejected)], None)
        return mqtt.MQTT_ERR_SUCCESS, 1

    def publish(self, topic, payload, qos, retain):
        self.mid += 1
        self.publications.append((topic, payload, qos, retain))
        if self.auto_ack and self.publish_rc == mqtt.MQTT_ERR_SUCCESS:
            # Deliberately exercise on_publish firing before publish() returns.
            self.on_publish(self, None, self.mid, SimpleNamespace(is_failure=False), None)
        return SimpleNamespace(mid=self.mid, rc=self.publish_rc)

    def disconnect(self):
        self.disconnects += 1
        self.on_disconnect(self, None, None, "intentional close", None)

    def loop_stop(self):
        self.stops += 1


@pytest.fixture(autouse=True)
def isolated_environment(monkeypatch):
    for name in ("MQTT_BROKER_HOST", "MQTT_BROKER_PORT", "MQTT_TOPIC_BASE", "MQTT_QOS"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setattr(config, "load_dotenv", lambda *args, **kwargs: None)


@pytest.fixture
def fake_paho(monkeypatch):
    fake = FakePaho()

    def construct(*args, **kwargs):
        fake.constructor = (args, kwargs)
        return fake

    monkeypatch.setattr(client.mqtt, "Client", construct)
    monkeypatch.setattr(client, "Event", ImmediateEvent)
    return fake


@pytest.fixture
def fake_clock(monkeypatch):
    import time

    class Clock:
        seconds = 0.0
        actions = {}

        def sleep(self, seconds):
            self.seconds += seconds
            for at in sorted(list(self.actions)):
                if at <= self.seconds + 1e-9:
                    self.actions.pop(at)()

    clock = Clock()
    monkeypatch.setattr(time, "monotonic", lambda: clock.seconds)
    monkeypatch.setattr(time, "time_ns", lambda: BASE_NS + int(clock.seconds * 1_000_000_000))
    monkeypatch.setattr(time, "sleep", clock.sleep)
    return clock
