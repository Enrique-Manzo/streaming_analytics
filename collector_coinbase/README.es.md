# Coinbase Trade Collector — Guía de Despliegue

## Estructura del proyecto

```
collector/
├── main.py            # Aplicación FastAPI, lifespan, punto de entrada
├── contract.py        # Contrato de datos Pydantic: TradeEvent
├── observability.py   # ObservabilityState + renderizador Prometheus
├── ws_collector.py    # Bucle WebSocket + procesamiento de mensajes
├── config.py          # Constantes (sobreescribibles mediante variables de entorno)
├── requirements.txt
└── .env               # Credenciales de la API — NO incluir en control de versiones
```

---

## Inicio rápido (local)

```bash
pip install -r requirements.txt
python main.py
```

Endpoints disponibles:
- `GET http://localhost:8000/metrics`      — Formato de texto Prometheus
- `GET http://localhost:8000/metrics/json` — Snapshot JSON
- `GET http://localhost:8000/health`       — Comprobación de estado

---

## Despliegue en una VM de GCP Compute Engine con PM2

El collector se ejecuta directamente en una VM de Compute Engine. PM2 gestiona el proceso y garantiza que se reinicie automáticamente tanto si se cae como si la VM se reinicia.

### Requisitos previos
- VM de Compute Engine en ejecución con Python 3 y Node.js instalados
- PM2 instalado globalmente (`npm install -g pm2`)
- Git instalado en la VM
- Credenciales configuradas en el archivo `.env` dentro del proyecto

### 1. Habilita las APIs de GCP necesarias (una sola vez)

```bash
gcloud services enable \
  compute.googleapis.com \
  --project=$PROJECT_ID
```

### 2. Clona el repositorio en la VM

Conéctate a la VM por SSH y clona el proyecto desde GitHub:

```bash
gcloud compute ssh <nombre-de-tu-vm> --zone=<tu-zona>

# En la VM:
git clone https://github.com/Enrique-Manzo/streaming_analytics.git
```

### 3. Instala las dependencias y configura las credenciales

```bash
pip install -r requirements.txt

# Crea el archivo .env con tus credenciales
cp .env.example .env   # o créalo directamente
nano .env
```

### 4. Lanza el proceso con PM2

```bash
pm2 start main.py --name coinbase-collector --interpreter python3
```

### 5. Configura PM2 para arrancar con el sistema

Esto garantiza que el proceso se reinicie automáticamente si la VM se reinicia:

```bash
pm2 startup
# Ejecuta el comando que PM2 te indique en la salida
pm2 save
```

### 6. Comandos útiles de PM2

```bash
pm2 status                         # Estado de todos los procesos
pm2 logs coinbase-collector        # Ver logs en tiempo real
pm2 restart coinbase-collector     # Reiniciar el proceso
pm2 stop coinbase-collector        # Detener el proceso
pm2 delete coinbase-collector      # Eliminar el proceso de PM2
```

### 7. Actualizar a una nueva versión

Para desplegar cambios, basta con hacer pull del repositorio y reiniciar el proceso:

```bash
cd coinbase-collector
git pull origin main
pip install -r requirements.txt   # Solo si cambiaron las dependencias
pm2 restart coinbase-collector
```

---

## Referencia de variables de entorno

| Variable | Valor por defecto | Descripción |
|----------|-------------------|-------------|
| `COINBASE_WS_URL` | `wss://ws-feed.exchange.coinbase.com` | Endpoint WebSocket de Coinbase |
| `THROUGHPUT_WINDOW_SECONDS` | `60` | Ventana deslizante para el cálculo de throughput |
| `METRICS_RESET_INTERVAL_SECONDS` | `3600` | Frecuencia de reinicio de las métricas en ventana (segundos) |
| `DEDUP_MAXLEN` | `50000` | Número máximo de IDs de trade/secuencia para deduplicación |
| `HOST` | `0.0.0.0` | Host de escucha de FastAPI |
| `PORT` | `8000` | Puerto de escucha de FastAPI |
| `WS_RECONNECT_DELAY` | `5` | Segundos de espera antes de reconectar tras una caída |

---

## Notas sobre el diseño de métricas

**Métricas en ventana temporal** (se reinician cada hora por defecto):
`schema_compliance_rate`, `duplicate_rate`, `dlq_rate`,
`throughput`, `freshness_at_ingestion`, `contract_violation_detection_time`

Ofrecen semántica de "¿cómo está funcionando el collector *ahora mismo*?",
que es la base adecuada para las reglas de alertas.

**Contadores acumulados** (nunca se reinician, para auditoría):
`total_received`, `total_valid`, `total_invalid`,
`total_duplicates`, `total_dlq`

Aumentan de forma monotónica durante toda la vida del proceso.
Al reiniciar el proceso de PM2 se resetean a 0 — si necesitas persistencia
entre reinicios, envíalos a Cloud Monitoring o Firestore al apagar el proceso.
