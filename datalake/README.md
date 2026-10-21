# Data Lake — capas

Los datos se organizan en tres capas dentro de `datalake/`. Los datos en sí **no se
versionan** (ver `.gitignore`); acá vive solo la estructura y su documentación.

| Capa | Ruta | Contenido |
| --- | --- | --- |
| Raw | `datalake/raw/` | Eventos tal como llegan de Kafka, sin transformar. Formato JSON, particionado por `dt` y `hour`. |
| Processed | `datalake/processed/` | Eventos parseados y validados, con esquema fijo, timestamps normalizados y duplicados eliminados. Formato Parquet, particionado por `dt`. |
| Curated | `datalake/curated/` | KPIs, alertas y reportes listos para consumo. Un directorio por salida (`kpi_rating_avg`, `kpi_top_reasons`, `kpi_trend`, `alerts`, `ratings_rejected`). |

## Convenciones

- Particiones con la forma `dt=YYYY-MM-DD/hour=HH` (raw) y `dt=YYYY-MM-DD` (processed, curated).
- Escritura en modo `append` desde Spark Structured Streaming.
- Checkpoints fuera del lake: `checkpoints/` en la raíz del repo (ignorado por git).
- Zona horaria de referencia: `UTC`. `occurred_at` siempre en UTC.
- Rechazos: todo evento que no cumpla el contrato se escribe en
  `curated/ratings_rejected` con el motivo del rechazo, no se borra.

## Retención de la demo

Los datos generados para la sustentación se pueden regenerar desde el producer, así
que el lake local se puede limpiar sin pérdida.
