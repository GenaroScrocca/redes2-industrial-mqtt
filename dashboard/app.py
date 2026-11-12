"""Run from the repository root: streamlit run dashboard/app.py."""

import sys
from pathlib import Path

# Streamlit executes a file, so make project imports independent of its script path.
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import streamlit as st

from dashboard.components import SIGNALS, diagnostics, header, pump_panel, signal_chart
from dashboard.runtime import DemoRuntime
from dashboard.state import DemoConfig
from src.messaging.config import MqttConfig


@st.cache_resource(scope="session", on_release=lambda runtime: runtime.stop(), show_spinner=False)
def session_runtime() -> DemoRuntime:
    return DemoRuntime()


def initialize_settings():
    if "mqtt_defaults" not in st.session_state:
        try:
            config = MqttConfig.from_env(qos=1)
        except ValueError as exc:
            config = MqttConfig(qos=1)
            st.session_state.config_error = f"Revisá la configuración MQTT: {exc}"
        st.session_state.mqtt_defaults = config
    return st.session_state.mqtt_defaults


def start_demo(runtime):
    try:
        config = DemoConfig(MqttConfig(
            host=st.session_state.broker_host.strip(), port=int(st.session_state.broker_port),
            topic=st.session_state.mqtt_topic.strip(), qos=st.session_state.demo_qos,
        ), interval=float(st.session_state.demo_interval))
        runtime.start(config)
        st.session_state.config_error = ""
    except ValueError as exc:
        st.session_state.config_error = str(exc)


def reset_demo(runtime):
    runtime.reset()
    st.session_state.config_error = ""


def main():
    st.set_page_config(page_title="P-101 · Industrial Pump Telemetry", page_icon="⚙", layout="wide")
    st.html(f"<style>{Path(__file__).with_name('styles.css').read_text(encoding='utf-8')}</style>")
    defaults = initialize_settings()
    runtime = session_runtime()
    st.session_state.demo_runtime = runtime

    @st.fragment(run_every=0.5)
    def live_panel():
        snapshot = runtime.state.snapshot()
        busy = runtime.busy
        st.html(header(snapshot))
        with st.container(key="controls"):
            start, stop, reset, qos, interval = st.columns([1.6, 1.15, .85, 1.15, 1.35], gap="medium")
            start.button("INICIAR FLUJO", type="primary", width="stretch", disabled=busy, on_click=start_demo, args=(runtime,))
            stop.button("DETENER", width="stretch", disabled=not snapshot.active, on_click=runtime.stop)
            reset.button("RESET", width="stretch", on_click=reset_demo, args=(runtime,))
            qos.radio("QoS", [0, 1], index=1, horizontal=True, key="demo_qos", disabled=busy)
            interval.number_input("Intervalo · segundos", value=0.5, min_value=0.1, max_value=10.0, step=0.1, format="%.1f", key="demo_interval", disabled=busy)
        with st.expander("Configuración avanzada · conexión MQTT", expanded=False):
            host, port, topic = st.columns([1, .7, 2.3])
            host.text_input("Host", value=defaults.host, key="broker_host", disabled=busy)
            port.number_input("Puerto", value=defaults.port, min_value=1, max_value=65535, step=1, key="broker_port", disabled=busy)
            topic.text_input("Topic de telemetría", value=defaults.topic, key="mqtt_topic", disabled=busy)
            st.caption("Mosquitto debe estar iniciado. La aplicación no controla el servicio del broker.")
        if st.session_state.get("config_error"):
            st.error(st.session_state.config_error)
        if snapshot.broker == "OFFLINE":
            st.warning(f"BROKER OFFLINE · {snapshot.error}")
        elif busy and not snapshot.active:
            st.caption("Finalizando los workers MQTT… Podés iniciar un nuevo flujo al terminar el cierre.")
        elif snapshot.active and snapshot.status != "OPERATING":
            st.caption("Esperando conexión y telemetría MQTT recibida. Los valores se completan al recibir datos.")
        st.html(pump_panel(snapshot))
        st.html('<div class="section-heading"><span><span class="section-index">02</span> TENDENCIAS EN TIEMPO REAL</span><small>ÚLTIMOS 200 MENSAJES · HORA UTC</small></div>')
        for offset in (0, 3):
            columns = st.columns(3, gap="small")
            for column, (field, title, unit, color) in zip(columns, SIGNALS[offset:offset + 3]):
                column.plotly_chart(signal_chart(snapshot, field, title, unit, color), key=f"chart_{field}", theme=None, width="stretch", config={"displayModeBar": False})
        st.html(diagnostics(snapshot))
        st.html('<div class="page-footer"><span>REDES DE DATOS II / TELEMETRÍA INDUSTRIAL</span><span>FUENTE DE DATOS · MENSAJES RECIBIDOS DESDE MOSQUITTO</span></div>')

    live_panel()


if __name__ == "__main__":
    main()
