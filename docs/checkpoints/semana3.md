# Checkpoint Semana 3 — Fallo controlado y recuperación (#17, #19)

## Tarea que falló

`crear_topic` del DAG `rapidgo_pipeline`.

## Motivo del fallo

Kafka detenido (`docker compose stop kafka`). El productor de topics no pudo conectar al broker.

## Mensaje real del log

```
kafka.errors.NoBrokersAvailable: NoBrokersAvailable
```

La task agotó los 3 intentos (1 ejecución + 2 `retries` configurados en `default_args`) y Airflow la marcó como `FAILED`. Ninguna tarea posterior se ejecutó (`ejecutar_ingesta`, `ejecutar_alertas`, `calcular_kpis`, `calidad_*`, `generar_reporte` quedaron bloqueadas por dependencia). No se corrompió ningún dato: cada corrida usa su propio topic, lake y checkpoints aislados por `run_id`.

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

Con esos retries, al restaurar Kafka la misma corrida reintentó `crear_topic` y completó el flujo completo sin intervención manual.

## Evidencia asociada

- PR #45 (Ivan, #15): https://github.com/AzopG/rapidgo-variant/pull/45 — DAG + stack + agregación K1/K2/K3.
- PR #46 (Andrés, #18): https://github.com/AzopG/rapidgo-variant/pull/46 — CI valida el DAG con DagBag + paridad de K3.
- PR #47 (Luis, #16): https://github.com/AzopG/rapidgo-variant/pull/47 — calidad `raw`/`curated` y reporte.
