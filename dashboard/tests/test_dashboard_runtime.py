import json
import time
from concurrent.futures import ThreadPoolExecutor

import pytest

from dashboard.runtime import DemoRuntime
from dashboard.state import DemoConfig


def published(fake_broker):
    return fake_broker.clients.get("publisher") and fake_broker.clients["publisher"].publications


def test_start_is_idempotent_even_with_concurrent_clicks(demo, fake_broker, wait_for):
    with ThreadPoolExecutor(max_workers=5) as pool:
        results = list(pool.map(lambda _: demo.start(DemoConfig(interval=.1)), range(10)))
    wait_for(lambda: published(fake_broker))
    assert results.count(True) == 1
    assert len(fake_broker.created) == 2
    assert all(client.retry_delay == {"min_delay": 1, "max_delay": 1} for client in fake_broker.created)


def test_generated_or_published_samples_never_update_view_until_received(demo, fake_broker, wait_for):
    assert demo.start(DemoConfig(interval=.1))
    wait_for(lambda: published(fake_broker))
    assert demo.state.snapshot().received == 0
    assert demo.state.snapshot().latest is None
    assert demo.state.snapshot().status == "STOPPED"
    payload = fake_broker.clients["publisher"].publications[0]
    assert payload["sent_at_unix_ns"] > 0
    fake_broker.clients["subscriber"].receive(json.dumps(payload).encode())
    snapshot = demo.state.snapshot()
    assert snapshot.received == 1
    assert snapshot.latest["pressure_bar"] == payload["pressure_bar"]
    assert snapshot.status == "OPERATING"


def test_subscriber_must_ack_subscription_before_generation(demo, fake_broker, wait_for):
    fake_broker.ack_subscribe = False
    demo.start(DemoConfig(interval=.1))
    wait_for(lambda: len(fake_broker.created) == 2)
    time.sleep(.15)
    assert not published(fake_broker)
    fake_broker.clients["subscriber"].ack_subscription()
    wait_for(lambda: published(fake_broker))


def test_stop_closes_both_clients_without_erasing_last_received_sample(demo, fake_broker, wait_for):
    fake_broker.deliver = True
    demo.start(DemoConfig(interval=.1))
    wait_for(lambda: demo.state.snapshot().received > 0)
    previous = demo.state.snapshot().received
    demo.stop()
    assert demo.wait_stopped()
    assert all(client.stopped and client.disconnected for client in fake_broker.created)
    assert demo.state.snapshot().received >= previous
    assert demo.state.snapshot().status == "STOPPED"
    assert not demo.busy


def test_reset_cleans_data_and_ignores_callbacks_from_old_run(demo, fake_broker, wait_for):
    fake_broker.deliver = True
    demo.start(DemoConfig(interval=.1))
    wait_for(lambda: demo.state.snapshot().received > 0)
    payload = json.dumps(fake_broker.clients["publisher"].publications[0]).encode()
    old_subscriber = fake_broker.clients["subscriber"]
    demo.reset()
    assert demo.wait_stopped()
    old_subscriber.receive(payload)
    assert demo.state.snapshot().received == 0
    assert demo.state.snapshot().latest is None
    assert demo.state.snapshot().status == "STOPPED"
    demo.start(DemoConfig(interval=.1))
    wait_for(lambda: len(fake_broker.created) == 4)
    old_subscriber.receive(payload)
    wait_for(lambda: demo.state.snapshot().received > 0)
    assert fake_broker.clients["publisher"].publications[0]["message_id"] == 1


def test_unavailable_broker_is_visible_without_stopping_ui_or_crashing(demo, fake_broker, wait_for):
    fake_broker.online = False
    demo.start(DemoConfig(interval=.1))
    wait_for(lambda: demo.state.snapshot().broker == "OFFLINE")
    assert demo.busy
    assert demo.state.snapshot().status == "BROKER OFFLINE"
    assert "Mosquitto" in demo.state.snapshot().error
    assert demo.state.snapshot().received == 0


def test_disconnect_deactivates_flow_then_reconnect_resubscribes_and_resumes(demo, fake_broker, wait_for):
    fake_broker.deliver = True
    demo.start(DemoConfig(interval=.1))
    wait_for(lambda: demo.state.snapshot().received > 0)
    for client in fake_broker.created:
        client.lose_connection()
    assert demo.state.snapshot().status == "BROKER OFFLINE"
    time.sleep(.15)
    count = demo.state.snapshot().received
    time.sleep(.15)
    assert demo.state.snapshot().received == count
    for client in fake_broker.created:
        client.connect()
    wait_for(lambda: demo.state.snapshot().received > count)
    assert demo.state.snapshot().status == "OPERATING"
    assert len(fake_broker.clients["subscriber"].subscriptions) == 2


@pytest.mark.parametrize("kind", ["foreign", "retained", "invalid_json", "nonfinite", "wrong_asset", "duplicate"])
def test_receiver_validates_and_filters_payloads(demo, fake_broker, wait_for, kind):
    demo.start(DemoConfig(interval=.1))
    wait_for(lambda: published(fake_broker))
    data = dict(fake_broker.clients["publisher"].publications[0])
    subscriber = fake_broker.clients["subscriber"]
    if kind == "foreign":
        data["demo_run_id"] = "other-session"
    elif kind == "nonfinite":
        data["pressure_bar"] = float("nan")
    elif kind == "wrong_asset":
        data["asset_id"] = "P102"
    encoded = b"invalid" if kind == "invalid_json" else json.dumps(data).encode()
    subscriber.receive(encoded, retained=kind == "retained")
    if kind == "duplicate":
        subscriber.receive(encoded)
        assert demo.state.snapshot().received == 2
        assert demo.state.snapshot().duplicates == 1
    else:
        assert demo.state.snapshot().received == 0
        assert demo.state.snapshot().rejected == int(kind in ("nonfinite", "wrong_asset", "invalid_json"))


def test_independent_sessions_keep_different_runs_and_data(demo, fake_broker, wait_for):
    other_broker = type(fake_broker)()
    other_broker.deliver = True
    other = DemoRuntime(client_factory=other_broker)
    try:
        demo.start(DemoConfig(interval=.1))
        other.start(DemoConfig(interval=.1))
        wait_for(lambda: published(fake_broker) and other.state.snapshot().received > 0)
        assert demo.state.snapshot().received == 0
        assert demo.state.snapshot().token != other.state.snapshot().token
        encoded = json.dumps(other_broker.clients["publisher"].publications[0]).encode()
        fake_broker.clients["subscriber"].receive(encoded)
        assert demo.state.snapshot().received == 0
    finally:
        other.stop()
        assert other.wait_stopped()


def test_startup_error_is_visible_and_worker_is_cleaned_up(fake_broker, wait_for):
    def failed_factory(*args, **kwargs):
        raise ConnectionRefusedError("No se pudo crear el cliente MQTT")

    runtime = DemoRuntime(client_factory=failed_factory)
    runtime.start(DemoConfig())
    wait_for(lambda: not runtime.busy)
    assert runtime.state.snapshot().broker == "OFFLINE"
    assert runtime.state.snapshot().status == "BROKER OFFLINE"
    runtime.stop()
    assert runtime.state.snapshot().status == "STOPPED"


def test_cleanup_continues_if_one_client_disconnect_fails(demo, fake_broker, wait_for, monkeypatch):
    demo.start(DemoConfig(interval=.1))
    wait_for(lambda: published(fake_broker))

    def failed_disconnect():
        raise RuntimeError("already disconnected")

    monkeypatch.setattr(fake_broker.clients["publisher"], "disconnect", failed_disconnect)
    demo.stop()
    assert demo.wait_stopped()
    assert all(client.stopped for client in fake_broker.created)
    assert fake_broker.clients["subscriber"].disconnected
