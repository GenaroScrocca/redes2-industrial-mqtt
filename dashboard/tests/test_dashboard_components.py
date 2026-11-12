from datetime import datetime
from base64 import b64decode
import re
from xml.etree import ElementTree

import pytest

from dashboard.components import SIGNALS, diagnostics, header, pump_panel, signal_chart
from dashboard.state import DemoConfig, DemoState
from src.messaging.config import MqttConfig


def test_stopped_pump_has_placeholders_and_no_fake_measurements():
    snapshot = DemoState().snapshot()
    html = pump_panel(snapshot)
    assert "P-101" in html and "CENTRIFUGAL PUMP" in html
    assert "STOPPED" in html and "ESPERANDO TELEMETRÍA MQTT" in html
    assert "5.20" not in html and "1,450" not in html
    assert "process-panel stopped" in html
    assert "<script" not in html


def test_only_received_values_render_on_the_central_pump(row):
    state = DemoState()
    state.begin("run", DemoConfig())
    for role in ("subscriber", "publisher"):
        state.link("run", role, True)
    state.subscribed("run")
    state.receive("run", {**row, "pressure_bar": 5.37})
    html = pump_panel(state.snapshot())
    svg = b64decode(re.search(r"base64,([^\"]+)", html).group(1)).decode()
    ElementTree.fromstring(svg)
    assert "5.37" in svg and "1,450" in svg
    assert "<img" in html and "<svg" not in html
    assert "process-panel running" in html
    assert "MQTT · RECEPCIÓN ACTIVA" in html


@pytest.mark.parametrize("field,title,unit,color", SIGNALS)
def test_empty_charts_do_not_contain_synthetic_traces(field, title, unit, color):
    figure = signal_chart(DemoState().snapshot(), field, title, unit, color)
    assert len(figure.data) == 0
    assert "Esperando" in figure.layout.annotations[0].text


def test_chart_contains_actual_received_timestamps_and_values(row):
    state = DemoState()
    state.begin("run", DemoConfig())
    state.receive("run", row)
    figure = signal_chart(state.snapshot(), *SIGNALS[0])
    assert list(figure.data[0].y) == [5.2]
    assert figure.data[0].x[0] == datetime.fromisoformat(row["received_at"])


def test_untrusted_topic_is_escaped_in_html():
    state = DemoState()
    state.begin("run", DemoConfig(MqttConfig(topic='<img src=x onerror="alert(1)">')))
    html = diagnostics(state.snapshot())
    assert "<img" not in html
    assert "&lt;img" in html


def test_broker_is_not_claimed_online_before_connecting():
    html = header(DemoState().snapshot())
    assert "SIN VERIFICAR" in html
    assert "<i></i>ONLINE" not in html
