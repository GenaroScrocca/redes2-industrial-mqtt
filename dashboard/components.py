"""Project-owned SVG/HTML panels and Plotly charts; no external assets."""

from base64 import b64encode
from html import escape
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go

from .state import Snapshot

SIGNALS = (
    ("pressure_bar", "PRESIÓN", "bar", "#d6ae88"),
    ("flow_lpm", "CAUDAL", "L/min", "#aebba1"),
    ("temperature_c", "TEMPERATURA", "°C", "#c39b7b"),
    ("vibration_mms", "VIBRACIÓN", "mm/s", "#c5baa2"),
    ("rpm", "VELOCIDAD", "RPM", "#bf9976"),
    ("latency_ms", "LATENCIA MQTT", "ms", "#a2b6ad"),
)


def number(value, decimals=2):
    return "—" if value is None else f"{value:,.{decimals}f}"


def header(snapshot: Snapshot) -> str:
    broker = {"IDLE": "SIN VERIFICAR", "CONNECTING": "CONECTANDO", "ONLINE": "ONLINE", "OFFLINE": "OFFLINE"}[snapshot.broker]
    broker_class = "good" if snapshot.broker == "ONLINE" else "bad" if snapshot.broker == "OFFLINE" else "neutral"
    pump = "OPERATING" if snapshot.status == "OPERATING" else "STOPPED"
    return f"""
    <div class="masthead"><div class="identity"><div class="brand-mark">II</div>
      <div><div class="eyebrow">REDES DE DATOS II <span>/</span> ESTACIÓN 01</div>
      <h1>P-101 <span>· INDUSTRIAL PUMP TELEMETRY</span></h1>
      <p>Estación de bombeo · Supervisión de telemetría MQTT en tiempo real</p></div></div>
      <div class="lab-tag"><span class="tiny-dot"></span> LOCAL LAB<br><small>MOSQUITTO / MQTT 3.1.1</small></div></div>
    <div class="status-strip">
      <div><span class="status-label">BROKER MQTT</span><strong class="{broker_class}"><i></i>{broker}</strong></div>
      <div><span class="status-label">PUMP STATUS</span><strong class="{'good' if pump == 'OPERATING' else 'neutral'}"><i></i>{pump}</strong></div>
      <div><span class="status-label">QUALITY OF SERVICE</span><strong>QoS {snapshot.config.mqtt.qos}</strong></div>
      <div><span class="status-label">TASA DE RECEPCIÓN · 5 s</span><strong>{number(snapshot.rate, 1)} <small>msg/s</small></strong></div>
    </div>"""


def _sensor(x, y, width, label, value, unit, tag):
    return f"""<g transform="translate({x} {y})" class="sensor">
      <rect width="{width}" height="83" rx="9"/><path d="M0 17 V66" class="sensor-accent"/>
      <text x="16" y="23" class="sensor-label">{label}</text>
      <text x="16" y="58" class="sensor-value">{value}<tspan class="sensor-unit" dx="8">{unit}</tspan></text>
      <text x="{width - 14}" y="23" text-anchor="end" class="sensor-tag">{tag}</text></g>"""


def pump_panel(snapshot: Snapshot) -> str:
    latest = snapshot.latest or {}
    active = snapshot.status == "OPERATING"
    state_class = "running" if active else "offline" if snapshot.broker == "OFFLINE" else "stopped"
    source = "MQTT · RECEPCIÓN ACTIVA" if active else "ÚLTIMOS DATOS RECIBIDOS" if latest else "ESPERANDO TELEMETRÍA MQTT"
    sensors = "".join((
        _sensor(414, 24, 172, "PRESIÓN", number(latest.get("pressure_bar")), "bar", "PT"),
        _sensor(47, 166, 185, "CAUDAL", number(latest.get("flow_lpm"), 1), "L/min", "FT"),
        _sensor(770, 126, 185, "TEMPERATURA", number(latest.get("temperature_c"), 1), "°C", "TT"),
        _sensor(239, 340, 187, "VIBRACIÓN", number(latest.get("vibration_mms")), "mm/s", "VT"),
        _sensor(599, 340, 180, "VELOCIDAD", number(latest.get("rpm"), 0), "RPM", "ST"),
    ))
    panel = f"""<section class="process-panel {state_class}">
    <div class="panel-heading"><div><span class="section-index">01</span> PROCESO <span class="panel-subtitle">/ CIRCUITO HIDRÁULICO</span></div>
      <span class="live-label"><i></i>{source}</span></div>
    <svg viewBox="0 0 1000 440" role="img" aria-label="P-101, bomba centrífuga. Estado: {snapshot.status}. Valores recibidos por MQTT.">
      <defs>
        <radialGradient id="spot"><stop stop-color="#6a4c35" stop-opacity=".28"/><stop offset="1" stop-color="#261e19" stop-opacity="0"/></radialGradient>
        <linearGradient id="metal" x2="0" y2="1"><stop stop-color="#5a4639"/><stop offset=".48" stop-color="#342b25"/><stop offset="1" stop-color="#28211d"/></linearGradient>
        <linearGradient id="rim" x2="1" y2="1"><stop stop-color="#c59a73"/><stop offset=".5" stop-color="#69503d"/><stop offset="1" stop-color="#aa7e5a"/></linearGradient>
        <pattern id="grid" width="28" height="28" patternUnits="userSpaceOnUse"><path d="M28 0H0V28" fill="none" stroke="#b09680" stroke-opacity=".035"/></pattern>
      </defs>
      <rect width="1000" height="440" fill="url(#grid)"/>
      <ellipse cx="530" cy="229" rx="295" ry="192" fill="url(#spot)"/>
      <path d="M110 276 H421 M500 156 V132 H693 V231 H890" class="pipe-shadow"/>
      <path d="M110 276 H421 M500 156 V132 H693 V231 H890" class="pipe"/>
      <path d="M110 276 H421 M500 156 V132 H693 V231 H890" class="flow-dash"/>
      <path d="M151 269 l10 7 -10 7 M822 224 l10 7 -10 7" class="flow-arrow"/>
      <text x="110" y="308" class="pipe-label">ENTRADA</text><text x="890" y="263" text-anchor="end" class="pipe-label">SALIDA</text>
      <path d="M500 108 V151 M233 208 H365 V267 M770 168 H731 V224 M333 340 V326 H455 M688 340 V325 H623" class="instrument-line"/>
      <rect x="554" y="205" width="108" height="70" rx="11" fill="url(#metal)" stroke="#705641" stroke-width="2"/>
      <rect x="654" y="214" width="23" height="52" rx="5" fill="#3f332a" stroke="#806044"/>
      <path d="M573 213V267 M586 211V269 M599 211V269 M612 211V269 M625 211V269 M638 213V267" stroke="#8b6a4d" stroke-opacity=".5" stroke-width="3"/>
      <path d="M455 299 L448 319 H480 L484 299 M615 276 V319 H642 V276" fill="#45352a" stroke="#74553d"/>
      <rect x="421" y="316" width="270" height="11" rx="4" fill="#574131" stroke="#9a7050"/>
      <circle cx="500" cy="230" r="77" fill="url(#metal)" stroke="url(#rim)" stroke-width="4"/>
      <circle cx="500" cy="230" r="65" fill="#2b231e" stroke="#826247" stroke-width="1"/>
      <g class="impeller"><circle cx="500" cy="230" r="52" fill="none" stroke="#a4805c" stroke-width="15" stroke-dasharray="13 34" opacity=".1"/></g>
      <circle cx="454" cy="183" r="3" fill="#bd956e"/><circle cx="546" cy="183" r="3" fill="#bd956e"/>
      <circle cx="454" cy="277" r="3" fill="#bd956e"/><circle cx="546" cy="277" r="3" fill="#bd956e"/>
      <text x="500" y="227" text-anchor="middle" class="pump-name">P-101</text>
      <text x="500" y="248" text-anchor="middle" class="pump-type">CENTRIFUGAL PUMP</text>
      <circle cx="468" cy="269" r="3" class="pump-led"/><text x="479" y="273" class="pump-small-state">{'RUNNING' if active else 'STANDBY'}</text>
      {sensors}
    </svg>
    <div class="process-footer"><span class="process-state"><i></i>{snapshot.status}</span>
      <span>PUBLISHER <b>→</b> MOSQUITTO <b>→</b> SUBSCRIBER <b>→</b> PANTALLA</span>
      <span>ASSET / P101</span></div></section>"""
    # st.html sanitizes away inline SVG. A self-contained SVG image preserves
    # the vector diagram and CSS animation without JavaScript or remote assets.
    before, _, rest = panel.partition("<svg ")
    svg_body, _, after = rest.partition("</svg>")
    styles = Path(__file__).with_name("styles.css").read_text(encoding="utf-8")
    svg = f'<svg xmlns="http://www.w3.org/2000/svg" class="{state_class}" {svg_body}</svg>'
    svg = svg.replace("<defs>", f"<style>{styles}</style><defs>", 1)
    encoded = b64encode(svg.encode("utf-8")).decode("ascii")
    return before + (
        f'<img class="process-diagram" src="data:image/svg+xml;base64,{encoded}" '
        f'alt="P-101 · CENTRIFUGAL PUMP · {snapshot.status}" />'
    ) + after


def signal_chart(snapshot: Snapshot, field: str, title: str, unit: str, color: str) -> go.Figure:
    figure = go.Figure()
    if snapshot.samples:
        frame = pd.DataFrame.from_records(snapshot.samples)
        figure.add_trace(go.Scatter(
            x=[stamp.to_pydatetime() for stamp in pd.to_datetime(frame["received_at"], utc=True)],
            y=frame[field], mode="lines",
            line={"color": color, "width": 2}, name=title,
            hovertemplate=f"%{{x|%H:%M:%S.%L}} UTC<br>%{{y:.3f}} {unit}<extra></extra>",
        ))
    else:
        figure.add_annotation(text="Esperando mensajes MQTT", x=0.5, y=0.5, xref="paper", yref="paper", showarrow=False, font={"color": "#8d7e70", "size": 12})
    figure.update_layout(
        title={"text": f"{title} <span style='color:#a08a76'>/ {unit}</span>", "font": {"size": 12, "color": "#ddc6af"}, "x": 0.06},
        height=218, margin={"l": 45, "r": 15, "t": 48, "b": 31},
        paper_bgcolor="#251e19", plot_bgcolor="#251e19", showlegend=False,
        font={"family": "Consolas, monospace", "size": 10, "color": "#a59483"},
        xaxis={"gridcolor": "#3c3027", "zeroline": False, "tickformat": "%H:%M:%S", "showticklabels": bool(snapshot.samples)},
        yaxis={"gridcolor": "#3c3027", "zeroline": False, "showticklabels": bool(snapshot.samples)},
        uirevision=snapshot.token or "empty", hovermode="x unified",
    )
    return figure


def diagnostics(snapshot: Snapshot) -> str:
    latest = snapshot.latest or {}
    cells = (
        ("MENSAJES RECIBIDOS", str(snapshot.received)),
        ("ÚLTIMO MESSAGE_ID", str(latest.get("message_id", "—"))),
        ("LATENCIA ACTUAL", number(latest.get("latency_ms")) + " ms"),
        ("LATENCIA MEDIA · SESIÓN", number(snapshot.latency_mean) + " ms"),
        ("P95 · VENTANA", number(snapshot.latency_p95) + " ms"),
        ("DUPLICADOS DETECTADOS", str(snapshot.duplicates)),
    )
    body = "".join(f"<div><span>{label}</span><strong>{value}</strong></div>" for label, value in cells)
    return f"""<section class="diagnostics"><div class="panel-heading"><div><span class="section-index">03</span> DIAGNÓSTICO MQTT</div>
      <span class="panel-subtitle">BUFFER {len(snapshot.samples)} / {snapshot.buffer_size}</span></div>
      <div class="diagnostic-cells">{body}</div>
      <div class="diagnostic-bottom"><span>TOPIC <code>{escape(snapshot.config.mqtt.topic)}</code></span>
      <span>QoS {snapshot.config.mqtt.qos} · DESCARTADOS {snapshot.rejected}</span></div>
      <div class="diagnostic-bottom"><span>ÚLTIMO TIMESTAMP UTC <code>{escape(str(latest.get('timestamp', '—')))}</code></span>
      <span>P95 desde 20 mensajes · Detección de IDs en ventana de {snapshot.buffer_size}</span></div></section>"""
