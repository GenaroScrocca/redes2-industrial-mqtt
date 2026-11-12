"""Two real MQTT clients and one publisher worker per Streamlit session.

No worker calls Streamlit. The subscriber callback is the only source of
visible data. Generation waits for SUBACK, and pauses during an outage.
"""

import json
import math
import time
from dataclasses import dataclass, field
from threading import Event, Lock, Thread
from uuid import uuid4

import paho.mqtt.client as mqtt

from src.messaging.telemetry import SIGNAL_FIELDS, decode_telemetry
from src.simulator import PumpSimulator
from src.simulator.config import ASSET_ID

from .state import DemoConfig, DemoState


@dataclass
class _Run:
    token: str
    config: DemoConfig
    state: DemoState
    client_factory: object
    stop: Event = field(default_factory=Event)
    subscriber_ready: Event = field(default_factory=Event)
    publisher_ready: Event = field(default_factory=Event)
    thread: Thread | None = None


def _make_client(run: _Run, role: str):
    client = run.client_factory(
        mqtt.CallbackAPIVersion.VERSION2, client_id=f"p101-{role}-{run.token[:16]}",
        clean_session=True, reconnect_on_failure=True,
    )
    client.connect_timeout = 2.0
    client.reconnect_delay_set(min_delay=1, max_delay=1)
    client.max_queued_messages_set(20)
    ready = run.subscriber_ready if role == "subscriber" else run.publisher_ready

    def offline(detail):
        ready.clear()
        run.state.link(run.token, role, False, detail)

    def connected(client, userdata, flags, reason_code, properties):
        if run.stop.is_set():
            return
        if reason_code.is_failure:
            offline(f"El broker rechazó la conexión: {reason_code}.")
            return
        run.state.link(run.token, role, True)
        if role == "subscriber":
            result, _ = client.subscribe(run.config.mqtt.topic, qos=run.config.mqtt.qos)
            if result != mqtt.MQTT_ERR_SUCCESS:
                offline("No se pudo suscribir al topic. Reintentando conexión.")
        else:
            ready.set()

    def subscribed(client, userdata, mid, reason_codes, properties):
        if run.stop.is_set():
            return
        if not reason_codes or any(code.is_failure for code in reason_codes):
            offline("El broker rechazó la suscripción. Revisá el topic y los permisos.")
        else:
            run.state.subscribed(run.token)
            ready.set()

    def received(client, userdata, message):
        received_ns = time.time_ns()
        if run.stop.is_set() or getattr(message, "retain", False):
            return
        try:
            data = json.loads(message.payload.decode("utf-8"))
            if not isinstance(data, dict) or data.get("demo_run_id") != run.token:
                return
            row = decode_telemetry(message.payload, received_ns)
            if row["asset_id"] != ASSET_ID or not all(
                math.isfinite(row[name]) for name in (*SIGNAL_FIELDS, "latency_ms")
            ):
                raise ValueError("Invalid P101 telemetry")
            run.state.receive(run.token, row)
        except (ValueError, TypeError, OverflowError, UnicodeError):
            run.state.reject(run.token)

    client.on_connect = connected
    client.on_connect_fail = lambda client, userdata: offline(
        f"Mosquitto no responde en {run.config.mqtt.host}:{run.config.mqtt.port}. "
        "Verificá que esté iniciado. La conexión se reintenta automáticamente."
    )
    client.on_disconnect = lambda client, userdata, flags, reason_code, properties: offline(
        "Se perdió la conexión con Mosquitto. Reintentando automáticamente."
    )
    if role == "subscriber":
        client.on_subscribe = subscribed
        client.on_message = received
    return client


def _serve(run: _Run):
    clients = []
    try:
        for role in ("subscriber", "publisher"):
            client = _make_client(run, role)
            clients.append(client)
            client.connect_async(run.config.mqtt.host, run.config.mqtt.port, keepalive=10)
            if client.loop_start() != mqtt.MQTT_ERR_SUCCESS:
                raise RuntimeError("No se pudo iniciar el cliente MQTT.")
        simulator = PumpSimulator(seed=run.config.seed)
        while not run.stop.is_set():
            if not run.subscriber_ready.is_set() or not run.publisher_ready.is_set():
                run.stop.wait(0.1)
                continue
            sample = simulator.next_sample()
            payload = {**sample, "demo_run_id": run.token, "sent_at_unix_ns": time.time_ns()}
            try:
                info = clients[1].publish(
                    run.config.mqtt.topic, json.dumps(payload, allow_nan=False),
                    qos=run.config.mqtt.qos, retain=False,
                )
            except (OSError, RuntimeError) as exc:
                run.publisher_ready.clear()
                run.state.link(run.token, "publisher", False, f"Publicación interrumpida: {exc}")
                continue
            if info.rc == mqtt.MQTT_ERR_NO_CONN:
                run.publisher_ready.clear()
                run.state.link(run.token, "publisher", False, "Conexión interrumpida. Reintentando.")
            elif info.rc != mqtt.MQTT_ERR_SUCCESS:
                run.state.link(run.token, "publisher", False, f"Error de publicación: {mqtt.error_string(info.rc)}")
            run.stop.wait(run.config.interval)
    except (OSError, RuntimeError, ValueError) as exc:
        run.state.link(run.token, "publisher", False, str(exc))
    finally:
        run.stop.set()
        for client in reversed(clients):
            try:
                client.disconnect()
            except (OSError, RuntimeError):
                pass
            finally:
                try:
                    client.loop_stop()
                except (OSError, RuntimeError):
                    pass
        run.state.worker_finished(run.token)


class DemoRuntime:
    def __init__(self, *, client_factory=None, buffer_size: int = 200):
        self.state = DemoState(buffer_size)
        self._factory = client_factory or mqtt.Client
        self._guard = Lock()
        self._run: _Run | None = None

    @property
    def busy(self) -> bool:
        with self._guard:
            return bool(self._run and self._run.thread and self._run.thread.is_alive())

    def start(self, config: DemoConfig) -> bool:
        with self._guard:
            if self._run and self._run.thread and self._run.thread.is_alive():
                return False
            token = uuid4().hex
            self.state.begin(token, config)
            run = _Run(token, config, self.state, self._factory)
            run.thread = Thread(target=_serve, args=(run,), name=f"p101-demo-{token[:8]}", daemon=True)
            self._run = run
            run.thread.start()
            return True

    def stop(self):
        with self._guard:
            if self._run:
                self._run.stop.set()
                self.state.stop(self._run.token)

    def reset(self):
        self.stop()
        self.state.reset()

    def wait_stopped(self, timeout: float = 5) -> bool:
        """For shutdown verification/tests; UI controls use nonblocking stop()."""
        with self._guard:
            thread = self._run.thread if self._run else None
        if thread:
            thread.join(timeout)
        return not self.busy
