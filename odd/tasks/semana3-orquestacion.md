# Semana 3 — Orquestación con Airflow (Issues #15 y #19, parte de Ivan)

Rama `feat/airflow-dag` sobre `main` en `638319d` (fase 0 mergeada en #44).
Estrategia de entrega: `ask-on-risk` — un PR para #15 (stack + DAG + agregación) y otro para #19 (ADR-003 + checkpoint), que depende de #17.
Runner: `pytest -q` desde `.venv`. El DAG se valida en el contenedor de Airflow; la validación en CI es de #18 (Andrés).

## Specs

### #15 — DAG de Airflow con 5 tasks, dependencias y retries

Texto del issue, literal:

- S1. "Al menos 5 tasks con dependencias explicitas."
- S2. "`retries` configurado y paso de informacion por XCom."
- S3. "DAG importable sin errores."
- S4. "Vista del grafo en la UI y corrida exitosa."

Requisito del plan de la semana (`docs/plan-semana3.md:25-26`), literal:

- S5. "Airflow en `docker-compose.yml`: webserver, scheduler y postgres."
- S6. "DAG con ≥5 tasks, dependencias explícitas, `retries` y paso de datos por XCom."
- S7. "DAG importable sin errores."

Alcance derivado del objetivo del issue, "Orquestar calidad y agregacion sobre el lake":

- S8. La agregación que el DAG orquesta es el cálculo batch de los tres KPIs documentados en `docs/kpis-alerts.md:14-18` (K1, K2, K3), que hoy no existe como código.

### #15 — decisión de ejecución (§6.1 del plan)

- S9. Los jobs de Spark corren **dentro** del contenedor de Airflow, sin montar el socket de Docker ni hacer docker-in-docker. Decisión del usuario: opción A.

### #19 — ADR-003 (parte de Ivan; el checkpoint depende de #17)

Texto del issue, literal:

- S10. "ADR-003 y `docs/checkpoints/semana3.md` con la tarea que falló, el mensaje real del log y el commit que lo resolvió."

Acotación para esta entrega: el ADR se escribe ahora; el checkpoint se completa cuando #17 exista.

- S11. El ADR documenta la estrategia de orquestación **y** la de recuperación ante fallos: contexto, opciones, decisión y trade-offs (`docs/plan-semana3.md:54`).

### Contrato con #16

- S12. Antes de que Luis empiece #16, la signatura exacta de las funciones de calidad queda escrita en el issue #16 (`docs/plan-semana3.md:56`).

## Tasks

| ID | Qué | Specs | Ruta | Estado |
| --- | --- | --- | --- | --- |
| T1 | `orchestration/pipeline.py`: contexto de corrida, comandos y entorno de Spark (puro, sin Airflow) | S1, S2, S9 | inline | ✅ `feat/airflow-dag` |
| T2 | `airflow/dags/rapidgo_pipeline.py`: 9 tasks, dependencias explícitas, `retries`, XCom | S1-S3, S6, S7 | inline | ✅ importa con stub de Airflow, 9 tasks |
| T3 | `spark/kpis_job.py` + `spark/kpi_rules.py`: agregación batch de K1/K2/K3 | S8 | inline | ✅ 15 tests de reglas |
| T4 | `docker/airflow/Dockerfile` + `docker/airflow/requirements.txt`: Java 17 + PySpark | S5, S9 | inline | ✅ compose config válido |
| T5 | `docker-compose.yml`: postgres, airflow-init, webserver, scheduler | S5 | inline | ✅ `docker compose config` exit 0 |
| T6 | Tests puros de `orchestration`, `kpi_rules`, backfill y estructura del DAG | S1, S2, S8 | inline | ✅ 205 passed (33 nuevos) |
| T7 | `docs/decisions/ADR-003-airflow-spark-execution.md` | S9, S11 | inline | ✅ |
| T8 | Contrato de calidad publicado en el issue #16 | S12 | inline | ✅ [comentario](https://github.com/AzopG/rapidgo-variant/issues/16#issuecomment-6648465184) |
| T9 | Evidencia: build de la imagen, grafo en la UI y corrida de las tasks que no dependen de #16 | S4 | inline | ✅ build OK; grafo con 9 tasks; 6 tasks success |
| T10 | Commit, PR y revisión de Andrés | S1-S12 | inline | ✅ PR [#45](https://github.com/AzopG/rapidgo-variant/pull/45), review pedida a Andrés |

## Log

- L1. Pedido del usuario, literal: "dale con la A". La opción A es la fila "Imagen propia de Airflow con Java + PySpark, `spark-submit --master local[*]` adentro" de `docs/plan-semana3.md:155`.
- L2. Alcance: #15 incluye la agregación porque el objetivo del issue dice "orquestar calidad y agregacion", y los tres KPIs están documentados pero no implementados (`grep kpi_` no encuentra cálculo en `spark/`).
- L3. `filter` del plan: la opción A decía `spark-submit`, pero el wheel de PySpark no instala `spark-submit` en el `PATH`. Se ejecuta `python <job>.py` y el master se pasa por `PYSPARK_SUBMIT_ARGS`. Mismo resultado, sin tarball de Spark.
- L4. Aislamiento por corrida: topic `rapidgo.ratings.run.<run_id>`, lake `datalake/runs/<run_id>/` y checkpoints `checkpoints/runs/<run_id>/`. Sin eso, `--once` con `startingOffsets=earliest` reprocesa el histórico y los conteos del reporte no se reproducen. Precedente: `verify_pipeline`.
- L5. K3 necesita datos de hace 2 h. El producer solo generaba `occurred_at` hacia adelante, así que se le agregó `--backfill-minutes`, retrocompatible y con tests propios. El DAG lo usa con 120 min por defecto (`RAPIDGO_BACKFILL_MINUTES`).
- L6. El contrato de #16 se publicó en el issue antes de que Luis empiece: `check_raw(lake_dir)`, `check_curated(lake_dir)` y `build_report(lake_dir, quality)`. Las tasks de calidad importan `quality` dentro de la función para que el DAG siga siendo importable mientras el módulo no exista.
- L7. Verificación local: `compileall` de `producer spark tests scripts orchestration airflow/dags` exit 0; `pytest -q` 205 passed; `docker compose config` exit 0; el DAG se importó con un stub de Airflow y creó las 9 tasks con dependencias.
- L8. Primer `airflow dags test` falló en `calcular_kpis`: `spark_job_command` le pasaba `--topic` y `--checkpoint-dir` al job batch, y `argparse` los rechaza. Fix: parámetro `streaming=False` en el builder, con test de contrato (`tests/test_kpis_job_contract.py`). Segunda corrida: `iniciar_corrida`, `crear_topic`, `generar_eventos` (300 publicados), `ejecutar_ingesta` (`raw=300 processed=300 rechazados=0`), `ejecutar_alertas` (19 alertas) y `calcular_kpis` (4460 filas; K3 `avg_now=2.7267`, `avg_previous=2.5772`, `delta=0.1495`, `state=estable`) en success. `calidad_raw`, `calidad_curated` y `generar_reporte` fallan con `ModuleNotFoundError: No module named 'quality'`, que es el estado esperado hasta #16.
