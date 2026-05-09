# Beam Pipelines — Guía de Despliegue

## Estructura del proyecto

```
beam_pipelines/
├── aapl_advanced/
│   ├── aapl_trades_pipeline_v2.py   # Pipeline AAPL: trades + predicción de volatilidad (RF)
│   └── requirements.txt
├── all_assets/
│   ├── crypto_pipeline.py           # Pipeline crypto: trades de Coinbase
│   ├── quotes_pipeline.py           # Pipeline quotes: cotizaciones Alpaca
│   └── trades_pipeline.py           # Pipeline trades: trades multi-símbolo Alpaca
└── news_advanced/
    ├── finbert_backbone/             # Archivos del backbone de FinBERT (subir a GCS)
    ├── news_pipeline_v3.py           # Pipeline de noticias: ingesta + inferencia FinBERT
    └── requirements.txt
```

---

## Cómo funcionan estas pipelines

Todas las pipelines de este proyecto son **pipelines de streaming de Apache Beam** que se ejecutan sobre **Google Cloud Dataflow**. Cada una lee mensajes de una suscripción de Pub/Sub, los procesa, y escribe los resultados en BigQuery.

El flujo general es:

```
Pub/Sub subscription
        │
        ▼
  Beam pipeline (Dataflow)
        │
        ├──► BigQuery (tabla principal)
        │
        └──► Pub/Sub DLQ topic (mensajes inválidos)
```

Las pipelines de `aapl_advanced` y `news_advanced` tienen ramas adicionales de inferencia ML (ver más abajo).

Cada pipeline es **independiente** y se despliega por separado ejecutando su propio `python <pipeline>.py`. No existe un despliegue conjunto.

---

## Requisitos previos

Antes de desplegar cualquier pipeline, asegúrate de tener lo siguiente:

- GCP project con billing habilitado y Dataflow API activa
- `gcloud` CLI autenticado (`gcloud auth login`)
- Python 3.10+ con Apache Beam instalado (`pip install apache-beam[gcp]`)
- Un bucket de GCS para staging y archivos temporales de Dataflow
- Las suscripciones de Pub/Sub correspondientes creadas (ver sección siguiente)
- Las tablas de BigQuery creadas con el schema definido en los data contracts de cada pipeline (ver sección siguiente)
- Para `aapl_advanced` y `news_advanced`: los artefactos de modelos subidos a GCS (ver sección de modelos)

---

## Suscripciones de Pub/Sub

Cada pipeline consume de una suscripción de Pub/Sub configurada como constante al inicio del archivo. Las suscripciones por defecto son:

| Pipeline | Suscripción por defecto |
|----------|------------------------|
| `aapl_trades_pipeline_v2.py` | `aapl_stock_trades-sub` |
| `trades_pipeline.py` | `stock_trades-sub` |
| `quotes_pipeline.py` | `stock_quotes-sub` |
| `crypto_pipeline.py` | `crypto_trades-sub` |
| `news_pipeline_v3.py` | `financial_news-sub` |

Cada pipeline también tiene configurado su propio **topic DLQ** al que se enrutan los mensajes que fallan validación. Los topics DLQ siguen la convención `<nombre>-dlq` (p. ej. `aapl-trades-dlq`, `news-dlq`).

Tienes dos opciones:

**Opción A — Usar las suscripciones por defecto.** Crea las suscripciones con los mismos nombres que aparecen en las constantes de cada archivo. Las suscripciones deben estar vinculadas a los topics que publican tus collectors.

**Opción B — Usar suscripciones propias.** Edita la constante `SUBSCRIPTION` al inicio de cada archivo y reemplázala por el path completo de tu suscripción:
```python
SUBSCRIPTION = "projects/<tu-project-id>/subscriptions/<tu-suscripción>"
```

---

## Tablas de BigQuery

Cada pipeline define su schema en el propio código y usa `CREATE_IF_NEEDED` para crear la tabla automáticamente al arrancar si no existe. Sin embargo, **se recomienda crear las tablas manualmente antes del primer despliegue** para evitar comportamientos inesperados en streaming inserts.

Los schemas están documentados en los data contracts de cada pipeline (constantes `*_SCHEMA` al inicio de cada archivo). Como referencia rápida:

| Pipeline | Dataset | Tabla(s) |
|----------|---------|----------|
| `aapl_trades_pipeline_v2.py` | `stock_data` | `trades`, `aapl_vol_predictions` |
| `trades_pipeline.py` | `stock_data` | `trades` |
| `quotes_pipeline.py` | `stock_data` | `quotes` |
| `crypto_pipeline.py` | `stock_data` | `crypto_trades` |
| `news_pipeline_v3.py` | `financial_data` | `financial_news` |

---

## Artefactos de modelos en GCS

Dos pipelines requieren modelos ML almacenados en GCS. Los workers de Dataflow los descargan automáticamente al arrancar mediante el SDK de `google-cloud-storage`. **Deben estar subidos antes de ejecutar la pipeline.**

### `aapl_advanced` — Clasificador Random Forest de volatilidad

Sube el archivo del modelo al bucket de staging de Dataflow:

```
gs://dataflow-staging-us-central1-476924094843/models/vol_classifier_aapl.joblib
```

El archivo fuente es `vol_classifier_aapl.joblib`. Para subirlo:

```bash
gsutil cp vol_classifier_aapl.joblib \
  gs://dataflow-staging-us-central1-476924094843/models/vol_classifier_aapl.joblib
```

Las constantes relevantes en `aapl_trades_pipeline_v2.py`:
```python
BUCKET     = "dataflow-staging-us-central1-476924094843"
MODEL_BLOB = "models/vol_classifier_aapl.joblib"
```

---

### `news_advanced` — FinBERT fine-tuneado para análisis de sentimiento

Esta pipeline requiere **tres artefactos** en GCS, todos bajo `gs://dataflow-staging-us-central1-476924094843/models/`:

| Artefacto | Ruta en GCS | Origen                                                              |
|-----------|-------------|---------------------------------------------------------------------|
| Backbone de FinBERT | `models/finbert/model/` (directorio) | Se puede descargar desde transformers                               |
| Pesos del modelo fine-tuneado | `models/news_classifier/model/model_weights.weights.h5` | `data_mining/news_data/finbert_sentiment_v3.zip` (extraer el `.h5`) |
| Tokenizador | `models/news_classifier/tokenizer/` (directorio) | Incluido en el backbone de FinBERT                                  |

Las constantes relevantes en `news_pipeline_v3.py`:
```python
BUCKET               = "dataflow-staging-us-central1-476924094843"
BACKBONE_GCS_PREFIX  = "models/finbert/model"
WEIGHTS_GCS_BLOB     = "models/news_classifier/model/model_weights.weights.h5"
TOKENIZER_GCS_PREFIX = "models/news_classifier/tokenizer"
```

Para subir el backbone (directorio completo):

```bash
gsutil -m cp -r news_advanced/finbert_backbone/* \
  gs://dataflow-staging-us-central1-476924094843/models/finbert/model/
```

Para subir los pesos del modelo fine-tuneado (extraer primero el `.h5` del zip):

```bash
unzip data_mining/news_data/finbert_sentiment_v3.zip -d /tmp/finbert_weights
gsutil cp /tmp/finbert_weights/model_weights.weights.h5 \
  gs://dataflow-staging-us-central1-476924094843/models/news_classifier/model/model_weights.weights.h5
```

Para subir el tokenizador:

```bash
gsutil -m cp -r news_advanced/finbert_backbone/tokenizer/* \
  gs://dataflow-staging-us-central1-476924094843/models/news_classifier/tokenizer/
```

> **Nota:** La pipeline de noticias requiere workers `n1-standard-4` (15 GB de RAM) para cargar FinBERT. Esto ya está configurado en las `PipelineOptions` del archivo.

---

## Despliegue de las pipelines

Cada pipeline se despliega de forma independiente. El runner es `DataflowRunner`, configurado directamente en el código dentro de la función `run()`.

### `all_assets` — Pipelines multi-símbolo

Estas pipelines no tienen dependencias de modelos. Basta con instalar los requisitos y ejecutar:

```bash
cd beam_pipelines/all_assets

pip install apache-beam[gcp]

# Desplegar cada pipeline de forma individual:
python crypto_pipeline.py
python quotes_pipeline.py
python trades_pipeline.py
```

### `aapl_advanced` — Pipeline AAPL con predicción de volatilidad

Asegúrate de haber subido `vol_classifier_aapl.joblib` a GCS antes de ejecutar.

```bash
cd beam_pipelines/aapl_advanced

pip install -r requirements.txt

python aapl_trades_pipeline_v2.py
```

Parámetros de Dataflow configurados en el código:

| Parámetro | Valor |
|-----------|-------|
| `runner` | `DataflowRunner` |
| `region` | `europe-west3` |
| `machine_type` | `e2-standard-2` |
| `job_name` | `aapl-trades-vol-pipeline-v2` |
| `temp_location` | `gs://dataflow-staging-us-central1-476924094843/tmp` |
| `staging_location` | `gs://dataflow-staging-us-central1-476924094843/staging` |

### `news_advanced` — Pipeline de noticias con inferencia FinBERT

Asegúrate de haber subido los tres artefactos de modelos a GCS antes de ejecutar.

```bash
cd beam_pipelines/news_advanced

pip install -r requirements.txt

python news_pipeline_v3.py
```

Parámetros de Dataflow configurados en el código:

| Parámetro | Valor |
|-----------|-------|
| `runner` | `DataflowRunner` |
| `region` | `europe-west3` |
| `machine_type` | `n1-standard-4` |
| `disk_size_gb` | `50` |
| `job_name` | `financial-news-pipeline` |
| `temp_location` | `gs://dataflow-staging-us-central1-476924094843/tmp` |
| `staging_location` | `gs://dataflow-staging-us-central1-476924094843/staging` |

---

## Referencia rápida de constantes por pipeline

Si necesitas adaptar alguna pipeline a un entorno diferente, estas son las constantes a modificar al inicio de cada archivo:

| Constante | Descripción |
|-----------|-------------|
| `PROJECT_ID` | ID del proyecto GCP |
| `SUBSCRIPTION` | Path completo de la suscripción de Pub/Sub |
| `BQ_TABLE` | Tabla de BigQuery destino (`project:dataset.table`) |
| `DLQ_TOPIC` | Topic de Pub/Sub para mensajes inválidos |
| `BUCKET` | Bucket de GCS para staging, tmp y modelos |
| `MODEL_BLOB` / `*_GCS_PREFIX` | Rutas de los artefactos de modelos en GCS (solo ML pipelines) |
