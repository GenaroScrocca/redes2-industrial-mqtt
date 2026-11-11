# Redes 2 - Industrial MQTT

Prueba de concepto de telemetría industrial para una estación de bombeo simulada.

## Arquitectura

Python Simulator -> MQTT -> Eclipse Mosquitto -> Python Subscriber -> Streamlit Dashboard

## Variables simuladas

- pressure
- flow
- temperature
- vibration
- rpm

## Tecnologías

- Python
- Paho MQTT
- Eclipse Mosquitto
- Streamlit
- Pandas
- Plotly
- Wireshark
- Pytest

## Objetivo

Evaluar el comportamiento de MQTT mediante pruebas de latencia, pérdida de mensajes, duplicados, reconexión y comparación entre QoS 0 y QoS 1.
