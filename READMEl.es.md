# Gobernanza y Confiabilidad en Analítica en Streaming

**Trabajo Final de Máster — Máster en Data Science, Universitat Oberta de Catalunya**
**Autor:** Enrique Manzo · **Tutor:** Rafael Luque Ocaña · **Área:** Streaming Analytics

---

## Descripción del proyecto

Este repositorio contiene el código y los datos entregables del Trabajo Final de Máster titulado *"Gobernanza y confiabilidad en analítica en streaming: diseño e implementación de un pipeline moderno con Data Contracts, Observabilidad end-to-end y Modelos Predictivos"*. El trabajo propone el diseño e implementación de una pipeline de datos en streaming para el monitoreo en tiempo real de activos financieros —acciones, criptomonedas y noticias de mercado— usando una arquitectura moderna basada en eventos sobre Google Cloud Platform. El sistema ingiere mensajes de distintas fuentes de datos, los preprocesa en streaming y los almacena en un entorno analítico que permite su exploración mediante dashboards interactivos.

El núcleo del proyecto se centra en demostrar que es posible construir una arquitectura de streaming madura, gobernada y auditable sin incurrir en una sobrecarga técnica desproporcionada. Para ello, se exploran tres pilares: el uso de **data contracts** para formalizar la estructura y semántica de los eventos en el punto de ingesta, herramientas de **observabilidad end-to-end** que permiten monitorizar en tiempo real la calidad, latencia, integridad y trazabilidad de los datos, y dos **módulos de aprendizaje automático** integrados directamente en la pipeline de streaming.

Como caso de uso experimental, se incorporan un clasificador de volatilidad de precios basado en Random Forest (aplicado sobre barras OHLCV de AAPL en ventanas de 60 minutos) y un modelo de análisis de sentimiento de noticias financieras basado en FinBERT fine-tuneado sobre un corpus de más de 187.000 artículos. La combinación de estos componentes busca validar que las arquitecturas de streaming pueden ser a la vez confiables, reproducibles y analíticamente útiles en entornos donde la calidad del dato es crítica.

---

## Contenido del repositorio

```
.
├── beam_pipelines/              # Pipelines de Apache Beam / Dataflow
│   ├── aapl_advanced/           #   Pipeline AAPL + predicción de volatilidad
│   ├── all_assets/              #   Pipelines multi-símbolo (crypto, quotes, trades)
│   └── news_advanced/           #   Pipeline de noticias + inferencia FinBERT
│
│ 
├── alpaca_collector/            #   WebSocket trades y quotes (Alpaca / IEX)
├── coinbase_collector/          #   WebSocket trades (Coinbase)
├── news_collector/              #   Scraping de noticias (Alpha Vantage, Cloud Run Job)
│
├── data_mining/                 # Proyectos de modelado ML
│   ├── news_data/               #   Fine-tuning FinBERT para sentimiento financiero
│   └── stock_data/              #   Entrenamiento del clasificador RF de volatilidad AAPL
│
├── scrapers/                    # Proyectos de recolección de datos históricos
│   ├── news_data/               #   Recolección histórica de noticias financieras
│   └── stock_data/              #   Captura de precios históricos AAPL (2024–presente)
│
└── datasets/                    # Datasets en formato Parquet
    ├── news_2018_2026.parquet   #   > 187.000 artículos de noticias financieras
    ├── AAPL_bars.parquet        #   > 376.000 barras históricas OHLCV de AAPL
    ├── all_symbols.parquet      # Cotizaciones históricas de varios activos
    └── all_symbols.csv          # Cotizaciones históricas de varios activos en formato csv.
```

### Entregables principales

**Pipelines de Beam (`beam_pipelines/`):** pipelines de streaming sobre Google Cloud Dataflow. Incluyen validación de datos, enriquecimiento, agregaciones temporales e inferencia ML en tiempo real. Ver `beam_pipelines/README.md` para instrucciones de despliegue.

**Collectors de datos (`collectors/`):** aplicaciones Python que ingieren datos en tiempo real desde Alpaca (stocks y quotes), Coinbase (crypto) y Alpha Vantage (noticias). Los collectors de activos financieros se ejecutan en una VM de Compute Engine gestionada con PM2. El collector de noticias se despliega como Cloud Run Job con ejecución programada. Ver los `README.md` individuales de cada collector para instrucciones de despliegue.

**Proyectos de data mining (`data_mining/`):** notebooks de Jupyter con el proceso completo de entrenamiento de los dos modelos. `news_sentiment/` cubre el fine-tuning de FinBERT sobre el corpus de noticias. `price_volatility/` cubre la ingeniería de features, el entrenamiento y la evaluación del clasificador Random Forest. Cada notebook contiene documentación detallada del proceso y los resultados.

**Proyectos de data scraping (`data_scraping/`):** notebooks y scripts para la recolección de datos históricos. `news_scraping/` recopila artículos de noticias financieras desde Alpha Vantage. `aapl_price_scraping/` captura barras históricas de precios de AAPL desde 2024 en adelante.

**Datasets (`datasets/`):** tres archivos Parquet listos para usar. El dataset de noticias contiene más de 187.000 artículos con metadatos y puntuaciones de sentimiento. El dataset de barras AAPL contiene más de 376.000 barras OHLCV de 1 minuto. El dataset multi-activo recoge cotizaciones históricas de los principales activos del S&P 500 y crypto.

---

## Arquitectura

El proyecto implementa una **arquitectura Kappa**, donde tanto el procesamiento histórico como el en tiempo real se modelan como flujos continuos de eventos. No existe una capa de procesamiento por lotes separada: toda la lógica de transformación, validación e inferencia ocurre en la pipeline de streaming.

### Flujo de activos financieros (acciones y criptomonedas)

```
┌─────────────────────────────────────────────────────────────────┐
│              VM de Compute Engine (gestionada con PM2)          │
│                                                                 │
│   ┌──────────────────────┐     ┌─────────────────────────────┐  │
│   │  Coinbase Collector  │     │     Alpaca Collector        │  │
│   │  (WebSocket crypto)  │     │  (WebSocket stocks/quotes)  │  │
│   │                      │     │                             │  │
│   │  ► Data Contracts    │     │  ► Data Contracts           │  │
│   │  ► Observabilidad    │     │  ► Observabilidad           │  │
│   └──────────┬───────────┘     └────────────────┬────────────┘  │
└──────────────┼──────────────────────────────────┼───────────────┘
               │                                  │
               ▼                                  ▼
    Pub/Sub: crypto_trades          Pub/Sub: stock_trades
                                    Pub/Sub: stock_quotes
                                    Pub/Sub: aapl_stock_trades
               │                                  │
               └─────────────────┬────────────────┘
                                 ▼
               ┌──────────────────────────────────────┐
               │         Google Cloud Dataflow        │
               │                                      │
               │  ┌────────────────────────────────┐  │
               │  │      crypto_pipeline.py        │  │
               │  │  Validación · Enriquecimiento  │  │
               │  └──────────────┬─────────────────┘  │
               │                 │                    │
               │  ┌──────────────▼──────────────────┐ │
               │  │      trades_pipeline.py         │ │
               │  │  Validación · Enriquecimiento   │ │
               │  │  Agregaciones temporales        │ │
               │  └──────────────┬──────────────────┘ │
               │                 │                    │
               │  ┌──────────────▼──────────────────┐ │
               │  │   aapl_trades_pipeline_v2.py    │ │
               │  │  Validación · Enriquecimiento   │ │
               │  │  Barras OHLCV 1-min             │ │
               │  │  ► RF Volatility Classifier     │ │
               │  └──────────────┬──────────────────┘ │
               └─────────────────┼────────────────────┘
                                 │
                                 ▼
                          BigQuery
               ┌──────────────────────────────────┐
               │  stock_data.crypto_trades        │
               │  stock_data.stock_trades         │
               │  stock_data.stock_quotes         │
               │  stock_data.aapl_vol_predictions │
               └──────────────────────────────────┘
```

### Flujo de noticias financieras

```
┌─────────────────────────────────────┐
│  Cloud Scheduler (cada 5 minutos)   │
└──────────────────┬──────────────────┘
                   │ dispara
                   ▼
┌─────────────────────────────────────┐
│       Cloud Run Job                 │
│       News Collector                │
│                                     │
│  ► Scraping Alpha Vantage API       │
│  ► Data Contracts (Pydantic)        │
│  ► Métricas → Pushgateway           │
└──────────────────┬──────────────────┘
                   │
                   ▼
        Pub/Sub: financial_news
                   │
                   ▼
┌─────────────────────────────────────┐
│       Google Cloud Dataflow         │
│       news_pipeline_v3.py           │
│                                     │
│  ► Validación y normalización       │
│  ► Deduplicación (ventana 12h)      │
│  ► FinBERT Sentiment Inference      │
└──────────────────┬──────────────────┘
                   │
                   ▼
          BigQuery
┌──────────────────────────────────┐
│  financial_data.financial_news   │
└──────────────────────────────────┘
```

### Flujo de observabilidad — activos financieros

```
┌──────────────────────────────────────────────────────────────────┐
│                VM de Compute Engine                              │
│                                                                  │
│  ┌───────────────────┐    ┌───────────────────┐                  │
│  │ Coinbase Collector│    │ Alpaca Collector  │                  │
│  │  FastAPI app      │    │  FastAPI app      │                  │
│  │  GET /metrics     │    │  GET /metrics     │                  │
│  └────────┬──────────┘    └────────┬──────────┘                  │
│           │                        │                             │
│           └──────────┬─────────────┘                             │
│                      │ scraping (pull)                           │
│                      ▼                                           │
│             ┌─────────────────┐                                  │
│             │   Prometheus    │  ← métricas de alta cardinalidad │
│             └────────┬────────┘                                  │
│                      │                                           │
│             ┌────────▼────────┐                                  │
│             │     Grafana     │  ← observabilidad + negocio      │
│             │   Dashboard     │    (datos de BigQuery)           │
│             └─────────────────┘                                  │
└──────────────────────────────────────────────────────────────────┘
```

### Flujo de observabilidad — noticias financieras

El collector de noticias se ejecuta como Cloud Run Job y no puede exponer un endpoint HTTP persistente para que Prometheus haga scraping. En su lugar, publica las métricas al finalizar cada ejecución en un **Prometheus Pushgateway** instalado en la VM compartida.

```
┌──────────────────────────────────────────┐
│          Cloud Run Job                   │
│          News Collector                  │
│                                          │
│  Al finalizar cada ejecución:            │
│  ► push de métricas al Pushgateway       │
└──────────────────┬───────────────────────┘
                   │ push (HTTP)
                   ▼
┌──────────────────────────────────────────┐
│              VM de Compute Engine        │
│                                          │
│  ┌──────────────────────┐                │
│  │  Prometheus Gateway  │ ← puerto 9091  │
│  └──────────┬───────────┘                │
│             │ scraping (pull)            │
│  ┌──────────▼───────────┐                │
│  │     Prometheus       │                │
│  └──────────┬───────────┘                │
│             │                            │
│  ┌──────────▼───────────┐                │
│  │       Grafana        │                │
│  │      Dashboard       │                │
│  └──────────────────────┘                │
└──────────────────────────────────────────┘
```

---

## Quick Start — prueba rápida con datos de Coinbase

Esta guía permite levantar el flujo de crypto de extremo a extremo en local (collector) y en GCP (pipeline y BigQuery) con la mínima configuración posible. Solo se necesita una cuenta de GCP autenticada; el collector de Coinbase no requiere credenciales de API.

### Requisitos previos

- `gcloud` CLI instalado y autenticado (`gcloud auth login` y `gcloud auth application-default login`)
- Python 3.10+ con `pip`
- Un proyecto de GCP con facturación habilitada y las APIs de Pub/Sub, Dataflow y BigQuery activas

### Paso 1 — Crear el topic y la suscripción de Pub/Sub

```bash
export PROJECT_ID=<tu-project-id>

gcloud pubsub topics create crypto_trades --project=$PROJECT_ID
gcloud pubsub subscriptions create crypto_trades-sub \
  --topic=crypto_trades \
  --project=$PROJECT_ID
```

### Paso 2 — Crear la tabla en BigQuery

Crea el dataset y la tabla con el schema que define el data contract de la pipeline:

```bash
bq mk --dataset $PROJECT_ID:stock_data

bq mk --table $PROJECT_ID:stock_data.crypto_trades \
  symbol:STRING,trade_id:STRING,price:FLOAT,size:FLOAT,side:STRING,\
  exchange_timestamp:TIMESTAMP,ingest_timestamp:FLOAT,\
  freshness_seconds:FLOAT,trade_value:FLOAT,pipeline_ingest_time:TIMESTAMP
```

### Paso 3 — Lanzar la pipeline de Dataflow

```bash
cd beam_pipelines/all_assets
pip install apache-beam[gcp]

# Edita la constante PROJECT_ID en crypto_pipeline.py si es necesario
python crypto_pipeline.py
```

La pipeline tardará unos minutos en aprovisionarse en Dataflow. Puedes seguir el progreso en la consola de GCP → Dataflow.

### Paso 4 — Arrancar el collector de Coinbase en local

En otra terminal:

```bash
cd collectors/coinbase_collector
pip install -r requirements.txt

# Solo necesitas indicar tu project ID
export GCP_PROJECT_ID=<tu-project-id>

python main.py
```

El collector abrirá una conexión WebSocket con Coinbase y comenzará a publicar trades en el topic `crypto_trades`. En pocos segundos deberías ver registros llegando a la tabla de BigQuery.

---

## Documentación adicional

Cada componente del proyecto dispone de su propio README con instrucciones detalladas de despliegue y configuración:

- `beam_pipelines/README.md` — despliegue de todas las pipelines de Dataflow, suscripciones, modelos en GCS y schemas de BigQuery
- `collectors/alpaca_collector/README.md` — despliegue del collector de stocks y quotes en Compute Engine con PM2
- `collectors/coinbase_collector/README.md` — despliegue del collector de crypto en Compute Engine con PM2
- `collectors/news_collector/README.md` — despliegue del collector de noticias como Cloud Run Job con Cloud Scheduler

Para información detallada sobre los modelos, el proceso de entrenamiento, los resultados y el análisis exploratorio de los datos, consulta los notebooks de Jupyter en `data_mining/` y `data_scraping/`.
