"""Stateful, bounded random walks for normal pump operation."""

import random
from collections.abc import Callable
from datetime import datetime, timezone

from .config import ASSET_ID, SIGNALS, STATE


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


class PumpSimulator:
    """Generate successive samples, starting at the nominal signal values.

    A seed reproduces signal values independently of other simulator instances.
    Timestamps follow the clock; tests can supply a fixed, timezone-aware clock.
    The interval is handled by the caller and does not scale the walk's steps.
    """

    def __init__(
        self,
        *,
        seed: int | None = None,
        clock: Callable[[], datetime] = _utc_now,
    ) -> None:
        self._random = random.Random(seed)
        self._clock = clock
        self._message_id = 0
        self._values = {name: config.base for name, config in SIGNALS.items()}

    def next_sample(self) -> dict[str, str | int | float]:
        """Return one JSON-compatible sample with a sequential ID and UTC time."""
        now = self._clock()
        if now.tzinfo is None or now.utcoffset() is None:
            raise ValueError("clock must return a timezone-aware datetime")
        timestamp = now.astimezone(timezone.utc).isoformat(timespec="milliseconds")

        if self._message_id > 0:
            for name, config in SIGNALS.items():
                step = self._random.uniform(-config.max_step, config.max_step)
                self._values[name] = min(
                    config.maximum, max(config.minimum, self._values[name] + step)
                )

        self._message_id += 1
        readings = {
            name: (
                int(round(self._values[name]))
                if config.decimals == 0
                else round(self._values[name], config.decimals)
            )
            for name, config in SIGNALS.items()
        }
        return {
            "asset_id": ASSET_ID,
            "message_id": self._message_id,
            "timestamp": timestamp,
            "state": STATE,
            **readings,
        }
