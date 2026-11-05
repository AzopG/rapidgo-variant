# Recuperación de un fallo real del pipeline (#17)

**Issue:** #17 — Fallo controlado y recuperación documentada
**Autor:** Andrés Maldonado
**Fecha:** 2026-11-05
**Base:** `main` en `60f0b25` (DAG de Airflow de #15) + PR #46 (CI que valida el DAG)

## El resultado en una línea

Con Kafka detenido, `crear_topic` se agota en 3 intentos y el DAG **no ejecuta ninguna de
las 7 tasks siguientes**; al volver a levantar Kafka, **las 6 tasks del pipeline se
recuperan sin tocar una línea de código**. Las 3 tasks de calidad siguen fallando, pero
por otro motivo y con otro dueño: el módulo `quality` es de #16 (Luis) y todavía no
existe.

## Camino rápido para verificarlo

```bash
# 1. El fallo: Kafka abajo, el DAG completo
docker compose stop kafka
docker compose run --rm --no-deps airflow-scheduler \
  airflow dags test rapidgo_pipeline "2026-11-05T13:00:00+00:00"

# 2. La corrección: Kafka arriba, la misma corrida
docker compose up -d --wait kafka
docker compose run --rm --no-deps airflow-scheduler \
  airflow dags test rapidgo_pipeline "2026-11-05T14:00:00+00:00"
```

En Windows hace falta `$env:AIRFLOW_UID='50000'` antes de los dos comandos; ver §6.

## 1. Escenario elegido

El issue ofrece tres: Kafka detenido, raw vacío o esquema inválido. Se eligió **Kafka
detenido** porque es el único que:

- falla en una task **de infraestructura** y no en una de datos, así que el error se
  puede leer sin ambigüedad;
- ejercita los `retries=2` que declara el DAG, en vez de fallar de una;
- se recupera con una acción **operativa** (levantar el servicio), sin cambios de código
  que oculten si la recuperación fue real.

Los otros dos escenarios terminan en las tasks de calidad y no se pueden distinguir del
`ModuleNotFoundError` de §5.

## 2. El fallo observado

`crear_topic` intenta tres veces y las tres termina con el mismo error. Los tres
`try_number` son 1, 2 y 3, con un minuto de espera entre ellos (`retry_delay` del DAG):

```text
[2026-11-05T16:53:35.693+0000] Marking task as UP_FOR_RETRY. dag_id=rapidgo_pipeline, task_id=crear_topic, execution_date=20261105T130000
[2026-11-05T16:54:53.726+0000] Marking task as UP_FOR_RETRY. dag_id=rapidgo_pipeline, task_id=crear_topic, execution_date=20261105T130000
[2026-11-05T16:56:12.641+0000] Marking task as FAILED.        dag_id=rapidgo_pipeline, task_id=crear_topic, execution_date=20261105T130000
```

La causa, desde `scripts/create_topic.py:302`:

```text
  File "/opt/rapidgo/scripts/create_topic.py", line 302, in main
    raise Errors.NoBrokersAvailable()
kafka.errors.NoBrokersAvailable: NoBrokersAvailable
```

`orchestration.pipeline.run_command` traduce el exit code 1 del subproceso a una
excepción explícita, que es el mensaje que queda en la task:

```text
RuntimeError: el comando fallo con exit code 1: /home/airflow/.local/bin/python -m scripts.create_topic \
  --topic rapidgo.ratings.run.20261105T165316Z --bootstrap-servers kafka:29092
```

Y el cierre del DagRun:

```text
Marking run <DagRun rapidgo_pipeline @ 2026-11-05 13:00:00+00:00: manual__2026-11-05T13:00:00+00:00, state:running> failed
DagRun Finished: dag_id=rapidgo_pipeline, state=failed, run_type=manual
```

Log completo: `C:\Users\pilar\AppData\Local\Temp\opencode\dag-fallo2.log` (local, fuera
del repo; no se versiona porque `*.log` está en `.gitignore`).

## 3. Tareas afectadas

Lo importante no es que falle una task, es **cuánto se detiene el pipeline**. Con
`crear_topic` en FAILED, Airflow no ejecuta ninguna task aguas abajo:

| Task | Estado con Kafka detenido | Por qué |
| --- | --- | --- |
| `iniciar_corrida` | SUCCESS | solo calcula rutas y crea directorios |
| `crear_topic` | **FAILED** (3 intentos) | `NoBrokersAvailable` |
| `generar_eventos` | no ejecutada | depende de `crear_topic` |
| `ejecutar_ingesta` | no ejecutada | ídem |
| `ejecutar_alertas` | no ejecutada | ídem |
| `calcular_kpis` | no ejecutada | ídem |
| `calidad_raw` | no ejecutada | ídem |
| `calidad_curated` | no ejecutada | ídem |
| `generar_reporte` | no ejecutada | depende de las dos de calidad |

Son **7 de 9 tasks** las que nunca corren. El aislamiento por corrida de
`orchestration.pipeline` hace que esto no deje basura a medias: cuando `crear_topic`
falla, todavía no se escribió nada en el lake.

## 4. La recuperación

Se levantó Kafka (`docker compose up -d --wait kafka` → `healthy` en 23 s) y se corrió el
**mismo DAG** con otro `execution_date`. Sin cambios de código, sin limpiar estado:

| Task | Con Kafka detenido | Con Kafka arriba |
| --- | --- | --- |
| `iniciar_corrida` | SUCCESS | SUCCESS |
| `crear_topic` | **FAILED** | **SUCCESS** |
| `generar_eventos` | no ejecutada | **SUCCESS** |
| `ejecutar_ingesta` | no ejecutada | **SUCCESS** |
| `ejecutar_alertas` | no ejecutada | **SUCCESS** |
| `calcular_kpis` | no ejecutada | **SUCCESS** |
| `calidad_raw` | no ejecutada | FAILED (`No module named 'quality'`) |
| `calidad_curated` | no ejecutada | FAILED (`No module named 'quality'`) |
| `generar_reporte` | no ejecutada | no ejecutada |

**La corrección del fallo es operativa y la recuperación es completa para las 6 tasks del
pipeline**: `crear_topic` crea el topic, el producer publica, la ingesta escribe el lake,
las alertas y los KPIs corren. El fallo de Kafka no dejó estado que impida reintentar.

## 5. Lo que todavía no cierra: las 3 tasks de calidad

Las tres tasks de calidad fallan, y **no por el fallo de Kafka**:

```text
ModuleNotFoundError: No module named 'quality'
```

`airflow/dags/rapidgo_pipeline.py` importa `quality.checks` y `quality.report` **dentro de
la función**, a propósito, para que el DAG siga siendo importable mientras #16 no exista.
El módulo `quality/` no está en `main`: #16 (Luis) sigue abierto y sin rama publicada.

Consecuencia concreta: el criterio "corrida posterior exitosa" de #17 **no se puede
cumplir hoy**. La corrida de §4 deja 6 tasks en SUCCESS y 3 en FAILED, así que el DagRun
termina `failed`. Faltan las tres tasks de calidad, no la recuperación.

Lo que falta es una sola cosa, cuando #16 esté en `main`:

```bash
docker compose run --rm --no-deps airflow-scheduler \
  airflow dags test rapidgo_pipeline "2026-11-05T15:00:00+00:00"
# esperado: las 9 tasks en SUCCESS y DagRun en success
```

Ese es el orden que fija el plan §4: #17 va último, después de #16 y del DAG.

## 6. Problema real encontrado en el camino: permisos del bind mount

La primera corrida no llegó ni a Kafka. Falló en `iniciar_corrida`:

```text
File "/opt/airflow/dags/rapidgo_pipeline.py", line 69, in iniciar_corrida
  ctx.checkpoint_path.mkdir(parents=True, exist_ok=True)
PermissionError: [Errno 13] Permission denied: '/opt/rapidgo/checkpoints/runs'
```

Causa: el contenedor corre como `uid=50000(airflow)` y la carpeta `checkpoints/` del repo
era del `uid 185` con modo `0755`, así que el `mkdir` no podía escribir. `datalake/` sí
funcionaba porque estaba en `0777`.

Esto **contradice ADR-003**, que afirma que el problema de permisos del bind mount "en
Windows no aplica". No aplica al usuario, pero sí a la carpeta que viene versionada: el
dueño del directorio en el checkout decide, no el sistema operativo. Se comprobó con:

```bash
docker compose run --rm --no-deps airflow-scheduler bash -lc \
  'id; ls -ld /opt/rapidgo/checkpoints; mkdir -p /opt/rapidgo/checkpoints/runs'
# uid=50000(airflow) ... drwxr-xr-x 1 185 185 ... checkpoints
# mkdir: cannot create directory '/opt/rapidgo/checkpoints/runs': Permission denied
```

Arreglo aplicado **local, no en el repo** (para no tocar archivos de #15 sin acordarlo):

```bash
docker run --rm --user root -v "$PWD:/opt/rapidgo" -w /opt/rapidgo alpine \
  chown -R 50000:0 checkpoints
```

Queda como deuda para #19/#20: o el `airflow-init` hace ese `chown`, o `docker-compose.yml`
documenta el paso. Mientras el checkout venga con `checkpoints/` de otro uid, la primera
corrida en cualquier máquina falla en la primera task.

## 7. Qué se puede verificar de este documento

- [x] El fallo es reproducible con los dos comandos del camino rápido.
- [x] El log del error y las tareas afectadas están transcriptos, con timestamps.
- [x] La misma corrida con Kafka arriba deja 6 tasks en SUCCESS sin cambios de código.
- [ ] Las 9 tasks en SUCCESS y el DagRun en `success` — **pendiente de #16**.

## Próximo paso

Cuando #16 esté en `main`, correr el DAG una vez más, agregar la tabla de las 9 tasks y
abrir el PR de #17 con `Closes #17`.
