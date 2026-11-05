# Checkpoint Semana 3 — Fallo controlado y recuperación (#17, #19)

## Tarea que falló

`crear_topic` del DAG `rapidgo_pipeline`.

## Motivo del fallo

Kafka detenido (`docker compose stop kafka`). El productor de topics no pudo conectar al broker.

## Mensaje real del log

```
kafka.errors.NoBrokersAvailable: NoBrokersAvailable
```

La task `crear_topic` agotó los 3 intentos (1 ejecución + 2 `retries`) y Airflow la marcó como `FAILED`:

```
Marking task as FAILED. dag_id=rapidgo_pipeline, task_id=crear_topic, run_id=manual__2026-11-05T10:00:00+00:00
```

Después de restaurar Kafka, la corrida de recuperación terminó con las 9 tareas en `SUCCESS`:

```
Marking task as SUCCESS. dag_id=rapidgo_pipeline, task_id=iniciar_corrida, ...
Marking task as SUCCESS. dag_id=rapidgo_pipeline, task_id=crear_topic, ...
Marking task as SUCCESS. dag_id=rapidgo_pipeline, task_id=generar_eventos, ...
Marking task as SUCCESS. dag_id=rapidgo_pipeline, task_id=ejecutar_ingesta, ...
Marking task as SUCCESS. dag_id=rapidgo_pipeline, task_id=calidad_raw, ...
Marking task as SUCCESS. dag_id=rapidgo_pipeline, task_id=ejecutar_alertas, ...
Marking task as SUCCESS. dag_id=rapidgo_pipeline, task_id=calcular_kpis, ...
Marking task as SUCCESS. dag_id=rapidgo_pipeline, task_id=calidad_curated, ...
Marking task as SUCCESS. dag_id=rapidgo_pipeline, task_id=generar_reporte, ...
```


## Corrida de reproducción

- Ejecución fallida: `airflow dags test rapidgo_pipeline 2026-11-05T10:00:00+00:00`
- Log: `logs/airflow/dags-test-fallo-kafka.log`
- Task afectada: `crear_topic` (try 3 de 3), luego el DAG se detuvo por dependencias.

## Recuperación

Se restauró Kafka (`docker compose start kafka`, esperado a `healthy`) y se volvió a lanzar el DAG:

- Ejecución exitosa: `airflow dags test rapidgo_pipeline 2026-11-05T23:00:00+00:00`
- Log: `logs/airflow/dags-test-recuperacion.log`
- Resultado: las 9 tareas en `SUCCESS`
  - `iniciar_corrida`, `crear_topic`, `generar_eventos` (300 publicados)
  - `ejecutar_ingesta` (`raw=300 processed=300 rechazados=0`)
  - `calidad_raw` (ok=True, 6/6 checks)
  - `ejecutar_alertas` (19 alertas)
  - `calcular_kpis` (4450 filas; K3 `state=estable`, `delta=0.1495`)
  - `calidad_curated` (ok=True, 7/7 checks; 19 alertas con `ALERT_SCHEMA`)
  - `generar_reporte` (escribe `curated/report/report.json`)

## Commit que resolvió el problema

No hubo cambio de código: la estrategia de recuperación ya existía en el DAG. El commit que la habilita es:

- `7bd61f8 feat(airflow): orchestrate the pipeline with a nine-task DAG`
  - `default_args.retries = 2` y `retry_delay = 1 min` en `rapidgo_pipeline.py`.
  - `max_active_runs = 1`, `catchup = False`, `schedule = None` (una corrida a la vez, sin backfill histórico).
  - Cada tarea es idempotente por construcción (checkpoints de ingesta/alertas, `overwrite` de KPIs, reescritura del reporte).

Con esos retries, al restaurar Kafka se lanzó una nueva corrida (`airflow dags test rapidgo_pipeline 2026-11-05T23:00:00+00:00`) que reintentó `crear_topic` y completó el flujo completo sin intervención manual.

## Evidencia asociada

- PR #45 (Ivan, #15): https://github.com/AzopG/rapidgo-variant/pull/45 — DAG + stack + agregación K1/K2/K3.
- PR #46 (Andrés, #18): https://github.com/AzopG/rapidgo-variant/pull/46 — CI valida el DAG con DagBag + paridad de K3.
- PR #47 (Luis, #16): https://github.com/AzopG/rapidgo-variant/pull/47 — calidad `raw`/`curated` y reporte.
