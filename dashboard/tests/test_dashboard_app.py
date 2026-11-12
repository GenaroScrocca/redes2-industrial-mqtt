from pathlib import Path

import pytest
import streamlit as st
from streamlit.testing.v1 import AppTest

from dashboard import runtime

APP = Path(__file__).resolve().parents[1] / "app.py"


@pytest.fixture
def app(fake_broker, monkeypatch):
    """Headless Streamlit tests. The Paho boundary is always replaced."""
    st.cache_resource.clear()
    monkeypatch.setattr(runtime.mqtt, "Client", fake_broker)
    instance = AppTest.from_file(str(APP), default_timeout=30).run()
    assert not instance.exception
    yield instance
    resource = instance.session_state.demo_runtime
    resource.stop()
    assert resource.wait_stopped()
    st.cache_resource.clear()


def test_app_initial_controls_defaults_and_six_charts(app, fake_broker):
    assert [button.label for button in app.button] == ["INICIAR FLUJO", "DETENER", "RESET"]
    assert not app.button[0].disabled and app.button[1].disabled
    assert app.radio[0].value == 1
    assert app.number_input(key="demo_interval").value == .5
    assert len(app.get("plotly_chart")) == 6
    assert fake_broker.created == []


def test_app_start_stop_reset_use_single_session_runtime(app, fake_broker, wait_for):
    fake_broker.deliver = True
    resource = app.session_state.demo_runtime
    app.button[0].click().run()
    wait_for(lambda: resource.state.snapshot().received > 0)
    app.run()
    assert app.session_state.demo_runtime is resource
    assert app.button[0].disabled
    assert len(fake_broker.created) == 2
    assert resource.state.snapshot().status == "OPERATING"
    app.button[1].click().run()
    assert resource.wait_stopped()
    app.run()
    assert resource.state.snapshot().status == "STOPPED"
    assert resource.state.snapshot().latest is not None
    app.button[2].click().run()
    assert resource.state.snapshot().received == 0
    assert resource.state.snapshot().latest is None
    assert not app.exception


def test_app_handles_unavailable_broker_and_reset_without_crashing(app, fake_broker, wait_for):
    fake_broker.online = False
    app.button[0].click().run()
    resource = app.session_state.demo_runtime
    wait_for(lambda: resource.state.snapshot().broker == "OFFLINE")
    app.run()
    assert any("BROKER OFFLINE" in warning.value for warning in app.warning)
    assert not app.exception
    app.button[2].click().run()
    assert resource.wait_stopped()
    assert resource.state.snapshot().status == "STOPPED"
    assert resource.state.snapshot().received == 0


def test_invalid_configuration_stays_in_ui_and_does_not_launch_workers(app, fake_broker):
    app.text_input(key="broker_host").set_value("").run()
    app.button[0].click().run()
    assert len(app.error) == 1
    assert fake_broker.created == []
    assert not app.exception
