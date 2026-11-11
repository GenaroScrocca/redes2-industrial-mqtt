"""Bounded connection/subscription waits and clean Paho network-loop shutdown."""

from threading import Event

import paho.mqtt.client as mqtt

from .config import MQTT_TIMEOUT_SECONDS, MqttConfig


class MqttError(RuntimeError):
    """An MQTT operation failed or was not acknowledged in time."""


class MqttSession:
    def __init__(self, config: MqttConfig) -> None:
        self.config = config
        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, reconnect_on_failure=False)
        self.client.connect_timeout = MQTT_TIMEOUT_SECONDS
        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect
        self.client.on_subscribe = self._on_subscribe
        self._connect_done = Event()
        self._subscribe_done = Event()
        self._closing = False
        self._error: str | None = None

    def _on_connect(self, client, userdata, flags, reason_code, properties) -> None:
        if reason_code.is_failure:
            self._error = f"Broker rejected connection: {reason_code}"
        self._connect_done.set()

    def _on_disconnect(self, client, userdata, flags, reason_code, properties) -> None:
        if not self._closing:
            self._error = f"MQTT connection closed: {reason_code}"
            self._connect_done.set()
            self._subscribe_done.set()

    def _on_subscribe(self, client, userdata, mid, reason_codes, properties) -> None:
        if not reason_codes or any(code.is_failure for code in reason_codes):
            self._error = f"Broker rejected subscription: {reason_codes}"
        self._subscribe_done.set()

    def check_connection(self) -> None:
        if self._error is not None:
            raise MqttError(self._error)

    def __enter__(self) -> "MqttSession":
        try:
            result = self.client.connect(self.config.host, self.config.port, keepalive=60)
            if result != mqtt.MQTT_ERR_SUCCESS:
                raise MqttError(f"Connection failed: {mqtt.error_string(result)}")
            result = self.client.loop_start()
            if result != mqtt.MQTT_ERR_SUCCESS:
                raise MqttError(f"Network loop failed: {mqtt.error_string(result)}")
            if not self._connect_done.wait(MQTT_TIMEOUT_SECONDS):
                raise MqttError("Timed out waiting for broker CONNACK")
            self.check_connection()
        except BaseException as exc:
            self.close()
            if isinstance(exc, OSError):
                raise MqttError(
                    f"Cannot connect to {self.config.host}:{self.config.port}: {exc}"
                ) from exc
            raise
        return self

    def subscribe(self) -> None:
        result, _ = self.client.subscribe(self.config.topic, qos=self.config.qos)
        if result != mqtt.MQTT_ERR_SUCCESS:
            raise MqttError(f"Subscription failed: {mqtt.error_string(result)}")
        if not self._subscribe_done.wait(MQTT_TIMEOUT_SECONDS):
            raise MqttError("Timed out waiting for broker SUBACK")
        self.check_connection()

    def close(self) -> None:
        self._closing = True
        try:
            self.client.disconnect()
        finally:
            self.client.loop_stop()

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.close()
