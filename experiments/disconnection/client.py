"""Experimental Paho callbacks; reconnects run independently of sampling."""

import time
from queue import Queue
from threading import Event

import paho.mqtt.client as mqtt

from src.messaging.config import MqttConfig

from .common import iso_from_ns


class ResilientClient:
    def __init__(self, config: MqttConfig, *, role: str, retry_interval: int = 1) -> None:
        if role not in ("publisher", "subscriber"):
            raise ValueError("role must be publisher or subscriber")
        if retry_interval < 1:
            raise ValueError("retry_interval must be positive")
        self.config, self.role = config, role
        self.events = Queue()
        self.messages = Queue()
        self.acknowledgements = Queue()
        self.connected = Event()
        self.ready = Event()
        self._ever_connected = False
        self._reported_loss = False
        self._attempts = 0
        self._closing = False
        self.error: str | None = None
        self.client = mqtt.Client(
            mqtt.CallbackAPIVersion.VERSION2, clean_session=True, reconnect_on_failure=True
        )
        self.client.connect_timeout = 2.0
        self.client.reconnect_delay_set(min_delay=retry_interval, max_delay=retry_interval)
        self.client.on_pre_connect = self._on_pre_connect
        self.client.on_connect = self._on_connect
        self.client.on_connect_fail = self._on_connect_fail
        self.client.on_disconnect = self._on_disconnect
        self.client.on_subscribe = self._on_subscribe
        self.client.on_publish = self._on_publish
        self.client.on_message = self._on_message

    def _event(self, event: str, detail: str = "") -> None:
        now = time.time_ns()
        self.events.put({
            "timestamp": iso_from_ns(now), "unix_ns": now, "role": self.role,
            "event": event, "detail": detail, "qos": self.config.qos, "topic": self.config.topic,
        })

    def _on_pre_connect(self, client, userdata) -> None:
        if self._closing:
            return
        self._attempts += 1
        self._event("CONNECT_ATTEMPT" if self._attempts == 1 else "RECONNECT_ATTEMPT")

    def _lost(self, detail: str) -> None:
        self.connected.clear()
        self.ready.clear()
        if not self._reported_loss and not self._closing:
            self._event("DISCONNECTED", detail)
            self._reported_loss = True

    def _on_connect_fail(self, client, userdata) -> None:
        self._lost(f"Cannot connect to {self.config.host}:{self.config.port}")

    def _on_connect(self, client, userdata, flags, reason_code, properties) -> None:
        if self._closing:
            return
        if reason_code.is_failure:
            self._lost(f"CONNACK rejected: {reason_code}")
            return
        self._event("RECONNECTED" if self._ever_connected else "CONNECTED")
        self._ever_connected = True
        self._reported_loss = False
        self.connected.set()
        if self.role == "subscriber":
            result, _ = client.subscribe(self.config.topic, qos=self.config.qos)
            if result != mqtt.MQTT_ERR_SUCCESS:
                self._lost(f"SUBSCRIBE failed: {mqtt.error_string(result)}")
                if result != mqtt.MQTT_ERR_NO_CONN:
                    self.error = f"SUBSCRIBE failed: {mqtt.error_string(result)}"
        else:
            self.ready.set()

    def _on_subscribe(self, client, userdata, mid, reason_codes, properties) -> None:
        if self._closing:
            return
        if not reason_codes or any(code.is_failure for code in reason_codes):
            self.error = f"SUBACK rejected: {reason_codes}"
            self._event("SUBSCRIBE_FAILED", self.error)
        else:
            self.ready.set()
            self._event("SUBSCRIBED")

    def _on_disconnect(self, client, userdata, flags, reason_code, properties) -> None:
        self._lost(str(reason_code))

    def _on_publish(self, client, userdata, mid, reason_code, properties) -> None:
        self.acknowledgements.put((mid, not reason_code.is_failure, time.time_ns()))

    def _on_message(self, client, userdata, message) -> None:
        self.messages.put((message.payload, time.time_ns()))

    def start(self) -> None:
        self.client.connect_async(self.config.host, self.config.port, keepalive=10)
        result = self.client.loop_start()
        if result != mqtt.MQTT_ERR_SUCCESS:
            raise RuntimeError(f"Cannot start MQTT loop: {mqtt.error_string(result)}")

    def stop(self) -> None:
        if self._closing:
            return
        self._closing = True
        try:
            self.client.disconnect()
        finally:
            self.client.loop_stop()
            self.connected.clear()
            self.ready.clear()
