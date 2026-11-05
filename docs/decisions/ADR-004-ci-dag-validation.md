# ADR-004 — Cómo valida el CI el DAG de Airflow

- **Estado:** aceptado
- **Fecha:** 2026-11-05
- **Semana:** 3
- **Issues:** #18, #15

## Contexto

El plan §6.2 dejó abierta una decisión: cómo se asegura el CI de que el DAG importa
antes del merge. El DAG vive en `airflow/dags/rapidgo_pipeline.py`, importa
`apache-airflow` 2.10.4 (la versión de la imagen de ADR-003, que corre sobre Python
3.12) y `orchestration.pipeline`, que es stdlib.

El job principal del CI corre Python 3.13 e instala `requirements.txt`, que no trae
Airflow. Hasta ahora, un DAG roto no rompía el CI: rompía la demo.

Las tres opciones del plan:

| Opción | A favor | En contra |
| --- | --- | --- |
| `apache-airflow` pinneado en el CI | Valida con el paquete real | Job más lento y más frágil |
| Stub liviano del módulo `airflow` | Rápido | Valida el import, no que el DAG sea válido |
| Construir la imagen en CI | La validación más fiel | Lo más caro de las tres |

## Decisión

**Instalar `apache-airflow==2.10.4` con el archivo oficial de constraints de Python
3.12 en un job aparte (`dag`), y validar con `DagBag`.**

1. El job `dag` de `.github/workflows/ci.yml` usa **Python 3.12**, porque Airflow 2.10.4
   no soporta 3.13. Es la misma versión que la imagen `apache/airflow:2.10.4-python3.12`
   de ADR-003; validar con otra sería validar otra cosa.
2. `scripts/validate_dag.py` carga el directorio con `DagBag` y revisa el contrato que
   el plan pide del DAG. Sale con código 0/1/2, como el resto de los scripts.
3. El job corre además `tests/test_dag_import.py` y `tests/test_validate_dag.py`: el
   primero valida el DAG real, el segundo prueba que el validador **falla** cuando el
   DAG está roto. Un paso de CI que nunca puede fallar no es una guarda.

### Por qué `DagBag` y no `airflow dags list-import-errors`

Lo medí: `airflow dags list-import-errors` sobre un `AIRFLOW_HOME` limpio termina con

```text
ERROR: You need to initialize the database. Please run `airflow db init`.
```

O sea que el job tendría que correr `airflow db migrate` antes para preguntar algo que
no depende de la base. `DagBag` parsea los archivos y deja los errores en
`import_errors` sin tocar la metadata; además permite afirmar el grafo (dependencias,
`retries`, `max_active_runs`), que es la mitad del valor.

### Qué valida el contrato

- El directorio importa sin errores (`import_errors` vacío).
- Existe `rapidgo_pipeline` y tiene al menos 5 tasks (el plan pide ≥5).
- Ninguna task queda aislada: hay una task inicial y una final.
- `default_args.retries >= 1` y `retry_delay` declarado.
- `max_active_runs == 1`, `catchup == False` y `schedule is None`.

Los tres últimos no son estilo: cada corrida crea su propio topic, su lake y sus
checkpoints. Dos corridas simultáneas o un `catchup` histórico multiplican datos que
nadie va a leer.

## Costo medido

| Medición | Resultado |
| --- | --- |
| `pip install apache-airflow==2.10.4` con constraints de 3.12 (`python:3.12-slim`) | **66 s**, **240 MB** en `site-packages` |
| Import del DAG real con ese paquete | `import_errors {}`, 9 tasks |
| Imagen de ADR-003 (para comparar) | 3.3 GB por corrida si se construyera en CI |

El job `dag` corre en paralelo con el principal, así que el tiempo de pared del CI no
suma los dos. El principal gana un `setup-java` de segundos porque la prueba de paridad
de K3 (#18) necesita una sesión de Spark real.

## Consecuencias

- Un import roto en `airflow/dags/*.py` falla el PR con el archivo y el mensaje del
  parser, no en la demo.
- El contrato del DAG queda escrito en un solo lugar (`scripts/validate_dag.py`) y se
  puede correr a mano: `python -m scripts.validate_dag`, con el paquete instalado.
- **Límite:** el CI valida que el DAG importe y cumpla el contrato; no valida que
  **corra**. Eso lo cubre la corrida del DAG y la evidencia de #15 y #17.
- **Límite:** la versión de Airflow está pinneada en dos lugares, la imagen
  (`docker/airflow/Dockerfile`) y el CI (`ci.yml`). Subir una sin la otra hace que el
  CI valide con una versión distinta a la que corre; si se sube, se suben las dos.
- **Límite:** las tasks de calidad de #16 importan `quality` dentro de la función. El
  CI no puede validar ese contrato porque el módulo todavía no existe; cuando exista,
  el job `dag` lo va a ejercitar el día que alguien lo importe en el DAG de verdad.

## Verificación

```bash
# local, con un entorno que tenga apache-airflow==2.10.4 sobre Python 3.12
python -m scripts.validate_dag
python -m pytest tests/test_dag_import.py tests/test_validate_dag.py -v

# CI: job "DAG de Airflow importable y valido (#18)" en la pestaña Actions
```
