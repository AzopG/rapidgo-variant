# Semana 3 — CI valida el DAG (#18) y fallo controlado (#17)

Objetivo: que el CI detecte un DAG roto antes del merge, y demostrar con evidencia real que una corrida fallida se recupera.
Ramas: `ci/validar-dag` (#18) y `feat/recuperacion-controlada` (#17). Un PR por issue, con `Closes #NN` en el cuerpo.
Runner: `python -m pytest` (CI, Python 3.13 + `pyspark==4.0.1`); el DAG se valida con `apache-airflow==2.10.4` en Python 3.12, que es la versión de la imagen de Airflow.

## Specs

S1 — #18: "Paso que valida la importación del DAG."
S2 — #18: "Nuevas pruebas de reglas de negocio corriendo en CI."
S3 — #18: "CI verde en el release de la semana."
S4 — #18: "Corrida de Actions con el paso nuevo" como evidencia.
S5 — #17: "Fallo reproducible (Kafka detenido, raw vacío o esquema inválido)."
S6 — #17: "Log real del error y tareas afectadas."
S7 — #17: "Corrida posterior exitosa tras la corrección."
S8 — #17: "Checkpoint con el log y el commit que resolvió el problema."
S9 — Plan §6.2: la decisión de cómo valida CI el DAG se toma en #18 y queda documentada.
S10 — Plan §3: nadie mergea su propio PR; `Closes #NN` en el cuerpo, no en un comentario.

Alcance autorizado: `scripts/validate_dag.py`, `tests/test_dag_import.py`, `tests/test_validate_dag.py`, `tests/test_kpi_trend_parity.py`, `.github/workflows/ci.yml`, `docs/decisions/ADR-004-*`, `docs/checkpoints/semana3.md` y `odd/`. Nada de `spark/`, `producer/`, `airflow/` ni `orchestration/`: son de Ivan y ya están en `main` (`60f0b25`).

## Tasks

| ID | Qué | Specs | Ruta | Estado |
| --- | --- | --- | --- | --- |
| T1 | ADR-004: decisión §6.2 con las tres opciones y el costo medido | S9 | inline | hecho (`docs/decisions/ADR-004-ci-dag-validation.md`) |
| T2 | `scripts/validate_dag.py`: `DagBag`, sin base de datos, códigos 0/1/2 | S1 | inline | hecho |
| T3 | `tests/test_dag_import.py` (positivo) y `tests/test_validate_dag.py` (negativo: un DAG roto tiene que fallar) | S1, S2 | inline | hecho: 10 passed con Airflow 2.10.4 |
| T4 | `tests/test_kpi_trend_parity.py`: el `state` de `k3_frame` contra `kpi_rules.trend_state` | S2 | inline | hecho: 9 passed con Spark 4.0.1 |
| T5 | Job `dag` en `ci.yml` (Python 3.12 + Airflow pinneado) y Java 17 en el job principal | S1, S3 | inline | hecho |
| T6 | Verificación: corrida de Actions del PR y de `main`, con el paso nuevo | S3, S4 | inline | pendiente |
| T7 | #17: fallo controlado, log real, tareas afectadas y corrida posterior exitosa | S5-S7 | inline | pendiente |
| T8 | `docs/checkpoints/semana3.md` con el log y el commit que resolvió el fallo | S8 | inline | pendiente |

## Log

L1 — Pedido del usuario: "Ya tenemos el plan de la semana 3 y en el repo tambien debe estar el plan vamos a realizar lo que nos corresponde solo lo asignado a mi (Andrés)".

L2 — Antes de empezar: revisé y mergeé el PR #45 de Ivan (`60f0b25`), que trae el DAG. Verifiqué con `DagBag` de `apache-airflow==2.10.4` sobre Python 3.12: `import_errors {}`, 9 tasks, `retries=2`, `max_active_runs=1`, `catchup=False`, `schedule=None`, y 208 tests en verde dentro de `apache/spark:4.0.1`. Sin eso no hay DAG que validar.

L3 — Decisión §6.2 (S9): instalar `apache-airflow==2.10.4` pinneado con el archivo de constraints oficial en un job aparte de Python 3.12, y validar con `DagBag` en vez de `airflow dags list-import-errors`. Motivo medido: `pip install apache-airflow==2.10.4` con constraints deja 240 MB de `site-packages` en 66 s y el `DagBag` valida el grafo, no solo el import; `airflow dags list-import-errors` exige `airflow db migrate` previo (lo comprobé: falla con "You need to initialize the database" sobre un `AIRFLOW_HOME` limpio). Un stub no valida nada del grafo y construir la imagen de 3.3 GB por corrida es lo más caro de las tres.

L4 — Evidencia de las verificaciones del bloque #18:

| Verificación | Entorno | Resultado |
| --- | --- | --- |
| `python -m scripts.validate_dag` | `apache/airflow:2.10.4-python3.12`, sin base inicializada | `DAG_OK dag=rapidgo_pipeline tasks=9 edges=9 retries=2` |
| `pytest tests/test_dag_import.py tests/test_validate_dag.py` | mismo contenedor | 10 passed |
| `pytest tests/test_kpi_trend_parity.py` | `apache/spark:4.0.1` | 9 passed |
| suite completa | `apache/spark:4.0.1` | 217 passed, 2 skipped (los dos módulos del DAG) |
| `python -m scripts.validate_dag` sin Airflow | `.venv` local | exit 2 con mensaje explícito |
| `compileall`, `verify_pipeline --structure-only`, `docker compose config` | local | exit 0 |

L5 — Trampa encontrada: `pytest.importorskip("airflow")` NO saltea en un entorno sin Airflow, porque `airflow/` del repo es un paquete de espacio de nombres y `import airflow` funciona. Hay que preguntar por `airflow.models`. Lo detectó la suite del contenedor de Spark (7 failed), no la del job `dag`.

