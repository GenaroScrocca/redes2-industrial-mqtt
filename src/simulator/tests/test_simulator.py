import json
import math
import re
from datetime import datetime, timedelta, timezone

import pytest

from src.simulator import PumpSimulator
from src.simulator.config import SIGNALS

FIXED_TIME = datetime(2026, 11, 11, 18, 0, 0, 123000, tzinfo=timezone.utc)


def fixed_clock() -> datetime:
    return FIXED_TIME


def test_initial_sample_has_expected_schema_and_nominal_values():
    sample = PumpSimulator(clock=fixed_clock).next_sample()

    assert sample == {
        "asset_id": "P101",
        "message_id": 1,
        "timestamp": "2026-11-11T18:00:00.123+00:00",
        "state": "NORMAL",
        "pressure_bar": 5.2,
        "flow_lpm": 120.0,
        "temperature_c": 60.0,
        "vibration_mms": 2.0,
        "rpm": 1450,
    }
    assert type(sample["rpm"]) is int
    assert json.loads(json.dumps(sample, allow_nan=False)) == sample


@pytest.mark.parametrize("seed", [0, 42, 101])
def test_long_walk_stays_in_bounds_and_changes_gradually(seed):
    simulator = PumpSimulator(seed=seed, clock=fixed_clock)
    previous = simulator.next_sample()
    changed = set()

    for _ in range(5000):
        sample = simulator.next_sample()
        assert sample["asset_id"] == "P101"
        assert sample["state"] == "NORMAL"
        assert type(sample["rpm"]) is int
        for name, config in SIGNALS.items():
            value = sample[name]
            assert math.isfinite(value)
            assert config.minimum <= value <= config.maximum
            # Two rounded readings can differ by up to one extra display unit.
            rounding_allowance = 10 ** -config.decimals
            assert abs(value - previous[name]) <= config.max_step + rounding_allowance + 1e-9
            if value != previous[name]:
                changed.add(name)
        previous = sample

    assert changed == set(SIGNALS)


def test_message_ids_increment_and_instances_start_independently():
    first = PumpSimulator(seed=1)
    second = PumpSimulator(seed=1)

    assert [first.next_sample()["message_id"] for _ in range(100)] == list(range(1, 101))
    assert second.next_sample()["message_id"] == 1


def test_default_timestamp_is_iso8601_utc_and_current():
    before = datetime.now(timezone.utc)
    sample = PumpSimulator().next_sample()
    after = datetime.now(timezone.utc)

    assert re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}\+00:00", sample["timestamp"])
    timestamp = datetime.fromisoformat(sample["timestamp"])
    assert timestamp.utcoffset() == timedelta(0)
    assert before - timedelta(milliseconds=1) <= timestamp <= after


def test_clock_is_read_for_each_sample_and_converted_to_utc():
    local_timezone = timezone(timedelta(hours=-3))
    times = iter(
        [
            datetime(2026, 11, 11, 15, 0, second, tzinfo=local_timezone)
            for second in range(2)
        ]
    )
    simulator = PumpSimulator(clock=lambda: next(times))

    assert simulator.next_sample()["timestamp"] == "2026-11-11T18:00:00.000+00:00"
    assert simulator.next_sample()["timestamp"] == "2026-11-11T18:00:01.000+00:00"


def test_naive_clock_is_rejected():
    simulator = PumpSimulator(clock=lambda: datetime(2026, 11, 11))

    with pytest.raises(ValueError, match="timezone-aware"):
        simulator.next_sample()


def test_seed_reproduces_full_samples_with_a_fixed_clock():
    first = PumpSimulator(seed=42, clock=fixed_clock)
    second = PumpSimulator(seed=42, clock=fixed_clock)

    for _ in range(100):
        assert first.next_sample() == second.next_sample()


def test_different_seeds_produce_different_signal_sequences():
    first = PumpSimulator(seed=1, clock=fixed_clock)
    second = PumpSimulator(seed=2, clock=fixed_clock)

    assert [first.next_sample() for _ in range(10)] != [second.next_sample() for _ in range(10)]
