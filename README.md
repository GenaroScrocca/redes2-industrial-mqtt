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

## Telemetria MQTT

Con Mosquitto escuchando en `localhost:1883`, ejecutar desde la raiz del
repositorio en dos terminales PowerShell. Primero iniciar el subscriber:

```powershell
.\.venv\Scripts\python.exe -m src.messaging.subscriber --expected-messages 10
```

Esperar el mensaje `Suscripto` y ejecutar el publisher en la otra terminal:

```powershell
.\.venv\Scripts\python.exe -m src.messaging.publisher --samples 10 --interval 1 --seed 42
```

Ambos usan el topic `industrial/station01/pump/P101/telemetry`. Leen `.env`
(opcional, ver `.env.example`) y las variables de entorno. Los argumentos
`--host`, `--port`, `--topic` (topic completo) y `--qos` tienen prioridad.
El subscriber guarda cada mensaje valido, incluidos duplicados, en un CSV
unico dentro de `results/raw/`. Imprime un resumen al terminar; omitir
`--expected-messages` para recibir hasta Ctrl+C.

Ejecutar todos los tests, sin necesitar un broker:

```powershell
.\.venv\Scripts\python.exe -m pytest -q --basetemp=.pytest_cache/mqtt-telemetry-tmp
```

## Analisis del experimento QoS

Con los CSV reales nombrados como `qos0_run1_500msg_seed42.csv` y
`qos1_run1_500msg_seed42.csv` (corridas 1, 2 y 3), ejecutar desde la raiz:

```powershell
.\.venv\Scripts\python.exe -m experiments.qos.analyze "results/raw/qos[01]_run[123]_500msg_seed42.csv" --expected-messages 500
```

Acepta tambien una o varias rutas CSV individuales. QoS y corrida se leen
del nombre; el rango esperado es `1..N` (`--expected-messages`, por defecto
500). Genera `qos_runs_*.csv` por corrida y `qos_aggregate_*.csv` por QoS en
`results/processed/`, sin sobrescribir informes anteriores. Para cambiar
el destino, usar `--output-dir`.
