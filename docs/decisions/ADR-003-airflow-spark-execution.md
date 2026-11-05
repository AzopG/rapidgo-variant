# ADR-003 — Ejecución de Spark desde Airflow y recuperación ante fallos

- **Estado:** aceptado
- **Fecha:** 2026-11-04
- **Semana:** 3
- **Issues:** #15 (DAG), #17 (fallo controlado), #19 (este ADR)

## Contexto

La Semana 2 dejó los jobs de PySpark corriendo en un contenedor Linux por invitation de
`scripts/run_spark_job.py`, que hace `docker run` **desde el host** (ADR-002). La Semana 3
tiene que orquestarlos con Airflow, y un contenedor de Airflow no puede ejecutar `docker`
sin que se le monte el socket del daemon.

Hay una segunda decisión pegada a la primera: el DAG va a reintentar tasks. Los jobs
actuales escriben estado (checkpoints, cooldown de alertas) y cualquier reintento tiene
que ser seguro. La estrategia de recuperación es parte de la decisión, no un detalle de
implementación posterior.

## Opciones consideradas

| Opción | Cómo funciona | A favor | En contra |
| --- | --- | --- | --- |
| **A. Imagen propia de Airflow con Java + PySpark** | Los jobs corren con `python <job>.py` dentro del contenedor de Airflow, con `--master local[*]` | Sin docker-in-docker; mismo camino para el scheduler y para una corrida manual; reproducible; el contenedor no puede tocar el daemon | Imagen más pesada; Spark comparte CPU y memoria con Airflow |
| B. Montar el socket de Docker y reusar `run_spark_job.py` | Airflow lanza `docker run` contra el daemon del host | Menos código nuevo | El contenedor de Airflow obtiene control del daemon: puede crear, borrar y montar cualquier cosa del host. Es un privilegio que no se justifica para correr un job |

Se elige **A**. B resuelve el problema con un privilegio equivalente a root en el host y
no aporta nada que A no consiga.

## Decisión

### Orquestación

1. `docker/airflow/Dockerfile` parte de `apache/airflow:2.10.4-python3.12` e instala
   `openjdk-17-jre-headless` y `pyspark==4.0.1`. PySpark 4.0 no arranca con Java 11 ni
   sin `JAVA_HOME`.
2. Los jobs se ejecutan como `python /opt/rapidgo/spark/<job>.py`. **No se usa
   `spark-submit`**: el wheel de PySpark no lo pone en el `PATH`, y no hace falta el
   tarball de Spark para una sesión local.
3. `PYSPARK_SUBMIT_ARGS="--master local[*] --packages
   org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1 pyspark-shell"` le dice a la sesión
   dónde está el conector de Kafka. Ivy resuelve las dependencias transitivas una vez y
   las deja en el volumen `airflow_ivy`.
4. El repositorio se monta en `/opt/rapidgo` y `PYTHONPATH=/opt/rapidgo`. Así el DAG
   importa `orchestration`, y los jobs encuentran `spark/` y `producer/`.
5. `docker-compose.yml` agrega `postgres`, `airflow-init`, `airflow-webserver` y
   `airflow-scheduler`. `airflow-init` corre `airflow db migrate` y crea el usuario
   `admin` una sola vez.

### Aislamiento por corrida

Cada corrida del DAG usa su propio `run_id` y, con él:

- su topic (`rapidgo.ratings.run.<run_id>`),
- su lake (`datalake/runs/<run_id>/`),
- sus checkpoints (`checkpoints/runs/<run_id>/`).

Sin esto, `--once` con `startingOffsets=earliest` sobre el topic principal reprocesa todo
el histórico y los conteos del reporte dejan de ser reproducibles. Es la misma decisión
que ya tomó `scripts/verify_pipeline.py` con `rapidgo.ratings.verify.<id>`.

### Recuperación ante fallos

- `retries=2` y `retry_delay=1 min` en `default_args` del DAG (`RAPIDGO_DAG_RETRIES`
  permite subirlo sin tocar código).
- `catchup=False` y `max_active_runs=1`: una corrida a la vez, sin backfill de fechas
  pasadas.
- Cada task es idempotente por construcción:
  - `ejecutar_ingesta` y `ejecutar_alertas` retoman desde el checkpoint de la corrida, no
    reprocesan lo ya escrito.
  - `calcular_kpis` escribe con `mode("overwrite")` sobre la salida de la corrida.
  - `calidad_*` y `generar_reporte` solo leen y reescriben el reporte.
- El cooldown de alertas se marca **después** de una escritura exitosa (fase 0, riesgo 1).
  Si `_emit` falla, el reintento vuelve a emitir la alerta en vez de perderla. Sin ese
  arreglo, `retries=2` habría sido una máquina de perder alertas.
- Un fallo se considera recuperado cuando la misma corrida, después de corregir la causa,
  termina en `success`. La evidencia (task, mensaje real del log, commit que lo resolvió)
  va en `docs/checkpoints/semana3.md` (#19).

## Consecuencias

### A favor

- El contenedor de Airflow no tiene privilegios sobre el daemon de Docker.
- El camino de ejecución es el mismo en local y en CI: un `python` con Java y PySpark.
- La imagen encapsula la versión de Java y de PySpark, que fue el riesgo principal de la
  Semana 1.

### En contra / límites

- La imagen crece: la base de Airflow más Java más el wheel de PySpark superan 1.5 GB.
- `local[*]` no es un cluster. Para el volumen del proyecto alcanza; no escala a datos
  que no entren en una máquina.
- La primera corrida necesita red para que Ivy baje el conector. Después vive en el
  volumen `airflow_ivy`.
- Spark comparte CPU y memoria con el scheduler porque `LocalExecutor` corre las tasks en
  el mismo contenedor. Para la demo es aceptable; un `CeleryExecutor` con workers
  separados sería el siguiente paso si hiciera falta.
- En Linux, el bind mount del repo lo escribe el uid 50000 del contenedor. `datalake/runs`
  y `checkpoints/runs` tienen que ser escribibles por ese uid; en Windows no aplica.

## Verificación

```bash
docker compose build airflow-init
docker compose up -d postgres airflow-init
docker compose up -d airflow-webserver airflow-scheduler
# UI: http://localhost:8080  (admin / admin)
# DAG: rapidgo_pipeline -> Trigger DAG
```

El DAG importa sin errores si aparece en la UI con las nueve tasks y el grafo dibujado.
La corrida completa depende de #16 (funciones de calidad); hasta que existan, las tasks
`calidad_raw`, `calidad_curated` y `generar_reporte` fallan con `ModuleNotFoundError` y
el resto del pipeline termina en `success`.
