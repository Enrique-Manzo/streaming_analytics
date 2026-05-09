# Alpaca Stock Collector — Guía de Despliegue

## Estructura del proyecto

```
alpaca-collector/
├── main.py            # Aplicación FastAPI, lifespan, punto de entrada
├── contract.py        # Contratos de datos Pydantic: TradeEvent y QuoteEvent
├── observability.py   # ObservabilityState y renderizador Prometheus (por stream)
├── ws_collector.py    # Bucle WebSocket, autenticación, enrutamiento y procesamiento de mensajes
├── config.py          # Constantes (sobreescribibles mediante variables de entorno o .env)
├── requirements.txt
└── .env               # Credenciales de la API — NO incluir en control de versiones
```

---

## Inicio rápido (local)

### 1. Configura tus credenciales

Edita el archivo `.env`:

```
ALPACA_API_KEY=tu_alpaca_api_key_aquí
ALPACA_API_SECRET=tu_alpaca_api_secret_aquí
GCP_PROJECT_ID=tu-gcp-project-id
```

### 2. Instala y ejecuta

```bash
pip install -r requirements.txt
python main.py
```

Endpoints disponibles:
| URL | Descripción |
|-----|-------------|
| `GET http://localhost:8000/metrics` | Formato de texto Prometheus (ambos streams) |
| `GET http://localhost:8000/metrics/json` | Snapshot JSON completo |
| `GET http://localhost:8000/metrics/trades` | Snapshot JSON solo de trades |
| `GET http://localhost:8000/metrics/quotes` | Snapshot JSON solo de quotes |
| `GET http://localhost:8000/health` | Comprobación de estado |

---

## Despliegue en una VM de GCP Compute Engine con PM2

El collector se ejecuta directamente en una VM de Compute Engine. PM2 gestiona el proceso y garantiza que se reinicie automáticamente tanto si se cae como si la VM se reinicia.

### Requisitos previos
- VM de Compute Engine en ejecución con Python 3 y Node.js instalados
- PM2 instalado globalmente (`npm install -g pm2`)
- Git instalado en la VM
- Dos topics de Pub/Sub creados: `stock_trades` y `stock_quotes`
- Credenciales configuradas en el archivo `.env` dentro del proyecto

### 1. Habilita las APIs de GCP necesarias (una sola vez)

```bash
gcloud services enable \
  compute.googleapis.com \
  pubsub.googleapis.com \
  --project=$PROJECT_ID
```

### 2. Crea los topics de Pub/Sub (una sola vez)

```bash
gcloud pubsub topics create stock_trades --project=$PROJECT_ID
gcloud pubsub topics create stock_quotes --project=$PROJECT_ID
```

### 3. Clona el repositorio en la VM

Conéctate a la VM por SSH y clona el proyecto desde GitHub:

```bash
gcloud compute ssh <nombre-de-tu-vm> --zone=<tu-zona>

# En la VM:
git clone https://github.com/Enrique-Manzo/streaming_analytics.git
```

### 4. Instala las dependencias y configura las credenciales

```bash
pip install -r requirements.txt

# Crea el archivo .env con tus credenciales
cp .env.example .env   # o créalo directamente
nano .env
```

### 5. Lanza el proceso con PM2

```bash
pm2 start main.py --name alpaca-collector --interpreter python3
```

### 6. Configura PM2 para arrancar con el sistema

Esto garantiza que el proceso se reinicie automáticamente si la VM se reinicia:

```bash
pm2 startup
# Ejecuta el comando que PM2 te indique en la salida
pm2 save
```

### 7. Comandos útiles de PM2

```bash
pm2 status                        # Estado de todos los procesos
pm2 logs alpaca-collector         # Ver logs en tiempo real
pm2 restart alpaca-collector      # Reiniciar el proceso
pm2 stop alpaca-collector         # Detener el proceso
pm2 delete alpaca-collector       # Eliminar el proceso de PM2
```

### 8. Actualizar a una nueva versión

Para desplegar cambios, basta con hacer pull del repositorio y reiniciar el proceso:

```bash
cd alpaca-collector
git pull origin main
pip install -r requirements.txt   # Solo si cambiaron las dependencias
pm2 restart alpaca-collector
```

---

## Contratos de datos

### TradeEvent (`T == "t"`)

| Campo | Tipo | Descripción |
|-------|------|-------------|
| `T` | `str` | Tipo de mensaje, siempre `"t"` |
| `S` | `str` | Símbolo bursátil (p. ej. `"AAPL"`) |
| `i` | `int` | ID del trade (clave de deduplicación) |
| `x` | `str` | Código de bolsa |
| `p` | `float` | Precio del trade |
| `s` | `int` | Volumen del trade (acciones) |
| `t` | `str` | Timestamp de la bolsa en ISO-8601 |
| `c` | `list[str]` | Códigos de condición del trade (opcional) |
| `z` | `str` | Identificador de cinta (opcional) |
| `ingest_timestamp` | `float` | Epoch Unix en el momento de recepción del mensaje |
| `freshness_seconds` | `float` | Latencia desde la bolsa hasta la ingesta |

### QuoteEvent (`T == "q"`)

| Campo | Tipo | Descripción |
|-------|------|-------------|
| `T` | `str` | Tipo de mensaje, siempre `"q"` |
| `S` | `str` | Símbolo bursátil |
| `bx` | `str` | Código de bolsa del bid |
| `bp` | `float` | Precio de bid |
| `bs` | `int` | Volumen de bid (acciones) |
| `ax` | `str` | Código de bolsa del ask |
| `ap` | `float` | Precio de ask |
| `as` | `int` | Volumen de ask (acciones) |
| `t` | `str` | Timestamp de la bolsa en ISO-8601 |
| `c` | `list[str]` | Códigos de condición del quote (opcional) |
| `z` | `str` | Identificador de cinta (opcional) |
| `ingest_timestamp` | `float` | Epoch Unix en el momento de recepción del mensaje |
| `freshness_seconds` | `float` | Latencia desde la bolsa hasta la ingesta |
| `spread` | `float` | Derivado: `ap - bp` |

---

## Topics de Pub/Sub

| Topic | Origen | Formato de mensaje |
|-------|--------|--------------------|
| `stock_trades` | `TradeEvent.model_dump()` serializado a JSON | Un trade por mensaje |
| `stock_quotes` | `QuoteEvent.model_dump(by_alias=True)` serializado a JSON | Un quote por mensaje |

---

## Métricas de observabilidad

Las métricas de Prometheus se dividen por stream (`trades` / `quotes`).

**Métricas en ventana temporal** (se reinician cada hora por defecto):

| Métrica | Descripción |
|---------|-------------|
| `alpaca_collector_trades_throughput_msgs_per_sec` | Mensajes de trade válidos por segundo (ventana deslizante de 60s) |
| `alpaca_collector_trades_schema_compliance_rate` | Fracción de trades que superan el contrato |
| `alpaca_collector_trades_duplicate_rate` | Fracción de trades marcados como duplicados |
| `alpaca_collector_trades_dlq_rate` | Fracción de trades enrutados a la DLQ |
| `alpaca_collector_trades_freshness_seconds_mean` | Latencia media bolsa→ingesta para trades |
| `alpaca_collector_trades_violation_detection_time_ms` | Tiempo medio en ms para detectar una violación de contrato en trades |
| `alpaca_collector_quotes_throughput_msgs_per_sec` | Mensajes de quote válidos por segundo |
| `alpaca_collector_quotes_schema_compliance_rate` | Fracción de quotes que superan el contrato |
| `alpaca_collector_quotes_duplicate_rate` | Fracción de quotes marcados como duplicados |
| `alpaca_collector_quotes_dlq_rate` | Fracción de quotes enrutados a la DLQ |
| `alpaca_collector_quotes_freshness_seconds_mean` | Latencia media bolsa→ingesta para quotes |
| `alpaca_collector_quotes_violation_detection_time_ms` | Tiempo medio en ms para detectar una violación de contrato en quotes |
| `alpaca_collector_quotes_mean_bid_ask_spread` | Spread medio bid-ask en dólares (solo quotes) |

**Contadores acumulados** (nunca se reinician):

`total_received`, `total_valid`, `total_invalid`, `total_duplicates`, `total_dlq` — un conjunto por stream.

---

## Referencia de variables de entorno

| Variable | Valor por defecto | Descripción |
|----------|-------------------|-------------|
| `ALPACA_API_KEY` | *(obligatorio)* | Clave de la API de Alpaca |
| `ALPACA_API_SECRET` | *(obligatorio)* | Secreto de la API de Alpaca |
| `ALPACA_WS_URL` | `wss://stream.data.alpaca.markets/v2/iex` | URL del WebSocket IEX de Alpaca |
| `GCP_PROJECT_ID` | `your-gcp-project-id` | Proyecto GCP para Pub/Sub |
| `TRADES_TOPIC_ID` | `stock_trades` | Topic de Pub/Sub para eventos de trade |
| `QUOTES_TOPIC_ID` | `stock_quotes` | Topic de Pub/Sub para eventos de quote |
| `THROUGHPUT_WINDOW_SECONDS` | `60` | Ventana deslizante para el cálculo de throughput |
| `METRICS_RESET_INTERVAL_SECONDS` | `3600` | Frecuencia de reinicio de las métricas en ventana (segundos) |
| `DEDUP_MAXLEN` | `50000` | Número máximo de claves en el buffer de deduplicación |
| `HOST` | `0.0.0.0` | Host de escucha de FastAPI |
| `PORT` | `8000` | Puerto de escucha de FastAPI |
| `WS_RECONNECT_DELAY` | `5` | Segundos de espera antes de reconectar tras una caída |

---

## Símbolos monitorizados

Top 20 del S&P 500 por capitalización de mercado:

`AAPL MSFT NVDA AMZN GOOGL META TSLA BRK.B AVGO JPM LLY UNH V XOM MA COST HD PG JNJ NFLX`
