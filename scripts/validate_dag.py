"""Valida el DAG de Airflow con el paquete real (Issue #18).

Un DAG roto tiene que romper el CI antes del merge, no la corrida de la demo. El
chequeo usa `DagBag`, el mismo parser que usa el scheduler, y no necesita base de
datos: `airflow dags list-import-errors` exige un `airflow db migrate` previo, y para
validar un grafo no hace falta.

Se revisa lo que el plan de la semana 3 pide del DAG (>=5 tasks, dependencias
explicitas, `retries` y paso de datos por XCom) y las invariantes que el resto del
diseno da por ciertas: una sola corrida activa, sin catchup y sin schedule, porque
cada corrida crea su propio topic y su propio lake.

Uso:
    python -m scripts.validate_dag
    python -m scripts.validate_dag --dags-dir airflow/dags --dag-id rapidgo_pipeline

Codigos de salida:
    0  el DAG importa y cumple el contrato
    1  el DAG importa pero no cumple alguna condicion, o no importa
    2  falta el paquete `apache-airflow` o el directorio de DAGs
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any, Sequence

REPO_ROOT = Path(__file__).resolve().parents[1]
DAGS_DIR = REPO_ROOT / "airflow" / "dags"
DAG_ID = "rapidgo_pipeline"
MIN_TASKS = 5

EXIT_OK = 0
EXIT_INVALID = 1
EXIT_UNAVAILABLE = 2


def load_dagbag(dags_dir: str | Path = DAGS_DIR):
    """Carga el directorio de DAGs con el parser real de Airflow.

    `DagBag` no toca la base: parsea los archivos y deja los errores en
    `import_errors`. Importar `airflow` aca adentro permite que el resto de la suite
    (que no instala Airflow) importe este modulo sin arrastrar el paquete.
    """
    from airflow.models import DagBag  # noqa: PLC0415

    return DagBag(dag_folder=str(dags_dir), include_examples=False)


def validate(bag: Any, dag_id: str = DAG_ID, min_tasks: int = MIN_TASKS) -> list[str]:
    """Problemas encontrados en el `DagBag`, en texto. Lista vacia = DAG sano."""
    problems: list[str] = []

    for path, error in sorted(bag.import_errors.items()):
        problems.append(f"no importa {path}: {error}")
    if problems:
        return problems

    dag = bag.dags.get(dag_id)
    if dag is None:
        return [f"no existe el DAG {dag_id!r}; encontrados: {sorted(bag.dags)}"]

    task_ids = list(dag.task_ids)
    if len(task_ids) < min_tasks:
        problems.append(
            f"el DAG tiene {len(task_ids)} tasks y el plan pide al menos {min_tasks}"
        )
    for task in dag.tasks:
        if not task.upstream_task_ids and not task.downstream_task_ids:
            problems.append(f"la task {task.task_id!r} no tiene dependencias")
    if not any(not task.upstream_task_ids for task in dag.tasks):
        problems.append("el DAG no tiene una task inicial (todas tienen upstream)")
    if not any(not task.downstream_task_ids for task in dag.tasks):
        problems.append("el DAG no tiene una task final (todas tienen downstream)")

    retries = dag.default_args.get("retries")
    if not isinstance(retries, int) or retries < 1:
        problems.append(f"default_args.retries tiene que ser >= 1, y es {retries!r}")
    if "retry_delay" not in dag.default_args:
        problems.append("default_args no declara retry_delay")

    # Una corrida a la vez y sin backfill: cada corrida publica en su propio topic y
    # escribe en su propio lake, asi que dos simultaneas o un catchup historico
    # multiplican los datos sin que nadie los lea.
    if dag.max_active_runs != 1:
        problems.append(
            f"max_active_runs tiene que ser 1, y es {dag.max_active_runs!r}"
        )
    if dag.catchup:
        problems.append("catchup tiene que ser False")
    if dag.schedule_interval is not None:
        problems.append(
            f"el DAG se dispara a mano: schedule tiene que ser None, y es "
            f"{dag.schedule_interval!r}"
        )

    return problems


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Valida que el DAG de Airflow importe y cumpla el contrato (#18)."
    )
    parser.add_argument("--dags-dir", default=str(DAGS_DIR))
    parser.add_argument("--dag-id", default=DAG_ID)
    args = parser.parse_args(argv)

    dags_dir = Path(args.dags_dir)
    if not dags_dir.is_dir():
        print(f"ERROR: no existe el directorio de DAGs {dags_dir}", file=sys.stderr)
        return EXIT_UNAVAILABLE

    try:
        bag = load_dagbag(dags_dir)
    except ModuleNotFoundError:
        print(
            "ERROR: falta apache-airflow; instalar "
            "`apache-airflow==2.10.4` con el archivo de constraints de Python 3.12",
            file=sys.stderr,
        )
        return EXIT_UNAVAILABLE

    problems = validate(bag, args.dag_id)
    if problems:
        for problem in problems:
            print(f"DAG_FALLO {problem}", file=sys.stderr)
        return EXIT_INVALID

    dag = bag.dags[args.dag_id]
    edges = sum(len(task.downstream_task_ids) for task in dag.tasks)
    print(
        f"DAG_OK dag={args.dag_id} tasks={len(dag.task_ids)} "
        f"edges={edges} retries={dag.default_args.get('retries')}",
        flush=True,
    )
    return EXIT_OK


if __name__ == "__main__":
    raise SystemExit(main())
