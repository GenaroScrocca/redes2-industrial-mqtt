"""Nominal values and bounds for the P101 pump, in each signal's units."""

from dataclasses import dataclass
from types import MappingProxyType
from typing import Mapping

ASSET_ID = "P101"
STATE = "NORMAL"
DEFAULT_SAMPLES = 10
DEFAULT_INTERVAL_SECONDS = 1.0


@dataclass(frozen=True)
class SignalConfig:
    base: float
    minimum: float
    maximum: float
    max_step: float
    decimals: int


# Steps describe the maximum change per sample, before display rounding.
SIGNALS: Mapping[str, SignalConfig] = MappingProxyType(
    {
        "pressure_bar": SignalConfig(5.2, 4.8, 5.6, 0.03, 3),
        "flow_lpm": SignalConfig(120.0, 110.0, 130.0, 1.0, 2),
        "temperature_c": SignalConfig(60.0, 55.0, 65.0, 0.15, 2),
        "vibration_mms": SignalConfig(2.0, 1.5, 2.5, 0.04, 3),
        "rpm": SignalConfig(1450.0, 1400.0, 1500.0, 5.0, 0),
    }
)
