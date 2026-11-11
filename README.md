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

## Simulador P101

Desde la raiz del repositorio en PowerShell, usando el entorno `.venv`:

```powershell
.\.venv\Scripts\python.exe -m src.simulator --samples 10 --interval 1 --seed 42
.\.venv\Scripts\python.exe -m pytest src/simulator/tests -q
```

Imprime una muestra JSON por linea. `--samples` debe ser un entero positivo
(por defecto 10); `--interval` indica segundos entre muestras (por defecto 1,
0 para generar sin espera). `--seed` es opcional y reproduce los valores de
las variables; los timestamps siguen el reloj actual en UTC.
