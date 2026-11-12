from concurrent.futures import ThreadPoolExecutor

import pytest

from dashboard.state import DemoConfig, DemoState
from src.messaging.config import MqttConfig


def ready(state):
    state.begin("run", DemoConfig())
    state.link("run", "publisher", True)
    state.link("run", "subscriber", True)
    state.subscribed("run")


def test_initial_state_has_no_telemetry_or_invented_metrics():
    snapshot = DemoState().snapshot()
    assert snapshot.status == "STOPPED"
    assert snapshot.broker == "IDLE"
    assert snapshot.latest is None
    assert snapshot.received == snapshot.rate == 0
    assert snapshot.latency_mean is snapshot.latency_p95 is None


def test_operating_requires_both_mqtt_links_suback_and_received_data(row):
    state = DemoState()
    ready(state)
    assert state.snapshot().status == "STOPPED"
    assert state.receive("run", row, arrived_at=10)
    assert state.snapshot(now=10).status == "OPERATING"
    state.link("run", "subscriber", False, "Broker no disponible")
    assert state.snapshot(now=10).status == "BROKER OFFLINE"
    assert state.snapshot().latest["pressure_bar"] == 5.2
    state.link("run", "subscriber", True)
    assert state.snapshot().broker == "OFFLINE"  # SUBACK still missing.
    state.subscribed("run")
    assert state.snapshot(now=10).status == "OPERATING"


def test_stale_reception_does_not_keep_flow_animation_active(row):
    state = DemoState()
    ready(state)
    state.receive("run", row, arrived_at=10)
    assert state.snapshot(now=20).status == "STOPPED"
    assert state.snapshot(now=20).broker == "ONLINE"


def test_rolling_buffer_and_duplicate_tracking_are_bounded(row):
    state = DemoState(buffer_size=20)
    ready(state)
    for message_id in range(1, 1001):
        state.receive("run", {**row, "message_id": message_id})
    snapshot = state.snapshot()
    assert snapshot.received == 1000
    assert len(snapshot.samples) == len(state._seen) == len(state._arrivals) == 20
    assert snapshot.samples[0]["message_id"] == 981
    state.receive("run", {**row, "message_id": 1000})
    assert state.snapshot().duplicates == 1
    state.receive("run", {**row, "message_id": 1})
    assert state.snapshot().duplicates == 1  # IDs outside the documented window expire.


def test_mean_is_cumulative_and_p95_uses_recent_received_samples(row):
    state = DemoState(buffer_size=20)
    ready(state)
    for message_id in range(1, 21):
        state.receive("run", {**row, "message_id": message_id, "latency_ms": float(message_id)})
        if message_id < 20:
            assert state.snapshot().latency_p95 is None
    snapshot = state.snapshot()
    assert snapshot.latency_mean == pytest.approx(10.5)
    assert snapshot.latency_p95 == pytest.approx(19.05)
    state.receive("run", {**row, "message_id": 21, "latency_ms": 21.0})
    assert state.snapshot().latency_mean == pytest.approx(11)
    assert state.snapshot().latency_p95 == pytest.approx(20.05)


def test_rate_counts_only_observed_receipts_over_elapsed_time(row, monkeypatch):
    monkeypatch.setattr("dashboard.state.time.monotonic", lambda: 0)
    state = DemoState()
    ready(state)
    assert state.snapshot(now=1).rate == 0
    state.receive("run", row, arrived_at=1)
    state.receive("run", {**row, "message_id": 2}, arrived_at=2)
    assert state.snapshot(now=2).rate == 1
    assert state.snapshot(now=10).rate == 0


def test_stop_retains_last_data_reset_clears_and_rejects_late_callbacks(row):
    state = DemoState()
    ready(state)
    state.receive("run", row)
    state.reject("run")
    state.stop("run")
    assert state.snapshot().latest is not None
    assert state.snapshot().status == "STOPPED"
    assert not state.receive("run", row)
    state.reset()
    snapshot = state.snapshot()
    assert snapshot.status == "STOPPED" and snapshot.broker == "IDLE"
    assert snapshot.latest is None
    assert snapshot.received == snapshot.duplicates == snapshot.rejected == 0
    assert snapshot.latency_mean is snapshot.latency_p95 is None
    state.link("run", "publisher", True)
    state.subscribed("run")
    assert not state.receive("run", row)
    assert state.snapshot().received == 0


def test_snapshot_and_input_mutation_cannot_change_shared_state(row):
    state = DemoState()
    ready(state)
    state.receive("run", row)
    row["pressure_bar"] = 999
    snapshot = state.snapshot()
    snapshot.samples[0]["pressure_bar"] = -1
    assert state.snapshot().latest["pressure_bar"] == 5.2


@pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf")])
def test_nonfinite_latency_is_rejected(row, value):
    state = DemoState()
    ready(state)
    assert not state.receive("run", {**row, "latency_ms": value})
    assert state.snapshot().received == 0
    assert state.snapshot().rejected == 1


def test_parallel_receipts_and_snapshots_are_safe(row):
    state = DemoState()
    ready(state)
    with ThreadPoolExecutor(max_workers=5) as pool:
        list(pool.map(lambda i: state.receive("run", {**row, "message_id": i}), range(1, 501)))
    assert state.snapshot().received == 500
    assert len(state.snapshot().samples) == 200


@pytest.mark.parametrize("interval,qos", [(0, 1), (float("nan"), 1), (11, 1), (.5, 2)])
def test_configuration_validation(interval, qos):
    with pytest.raises(ValueError):
        DemoConfig(MqttConfig(qos=qos), interval=interval)
