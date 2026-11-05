"""El validador del DAG tiene que fallar cuando el DAG está roto (Issue #18).

Un paso de CI que solo imprime "ok" no sirve: estas pruebas construyen DAGs de
mentira en un directorio temporal y comprueban que `validate` los rechaza por el motivo
correcto, y que no rechaza un DAG sano. Sin esto, el paso del CI podría estar pasando
siempre y nadie lo notaría.
"""

from __future__ import annotations

import textwrap

import pytest

# `airflow.models` y no `airflow`: el directorio `airflow/` del repo es un paquete de
# espacio de nombres, asi que `import airflow` funciona aunque el paquete no exista.
pytest.importorskip(
    "airflow.models",
    reason="apache-airflow solo se instala en el job `dag` del CI",
)

from scripts.validate_dag import load_dagbag, validate  # noqa: E402

VALIDO = textwrap.dedent(
    '''
    from datetime import datetime, timedelta

    from airflow import DAG
    from airflow.operators.python import PythonOperator


    def noop(**_):
        return None


    with DAG(
        dag_id="prueba",
        start_date=datetime(2026, 1, 1),
        schedule=None,
        catchup=False,
        max_active_runs=1,
        default_args={"retries": 2, "retry_delay": timedelta(minutes=1)},
    ) as dag:
        a = PythonOperator(task_id="a", python_callable=noop)
        b = PythonOperator(task_id="b", python_callable=noop)
        c = PythonOperator(task_id="c", python_callable=noop)
        d = PythonOperator(task_id="d", python_callable=noop)
        e = PythonOperator(task_id="e", python_callable=noop)

        a >> [b, c] >> d >> e
    '''
)

# Tiene que mencionar `airflow` y `dag`: el heurístico de DagBag ignora en silencio los
# archivos que no las contienen, y entonces el import roto nunca llega a `import_errors`.
ROTO = textwrap.dedent(
    '''
    from airflow import DAG  # noqa: F401

    raise RuntimeError("este DAG no importa a proposito")
    '''
)


def _escribir(tmp_path, nombre: str, codigo: str):
    destino = tmp_path / "dags"
    destino.mkdir(exist_ok=True)
    (destino / f"{nombre}.py").write_text(codigo, encoding="utf-8")
    return destino


def test_un_dag_sano_no_tiene_problemas(tmp_path):
    dags_dir = _escribir(tmp_path, "sano", VALIDO)
    bag = load_dagbag(dags_dir)
    assert bag.import_errors == {}
    assert validate(bag, dag_id="prueba") == []


def test_un_dag_que_no_importa_se_reporta(tmp_path):
    dags_dir = _escribir(tmp_path, "roto", ROTO)
    bag = load_dagbag(dags_dir)
    problemas = validate(bag, dag_id="prueba")
    assert any("no importa" in problema for problema in problemas), problemas
    assert any("no importa a proposito" in problema for problema in problemas), problemas


def test_un_dag_con_menos_de_cinco_tasks_se_reporta(tmp_path):
    chico = VALIDO.replace(
        '    e = PythonOperator(task_id="e", python_callable=noop)\n', ""
    ).replace("a >> [b, c] >> d >> e", "a >> [b, c] >> d")
    dags_dir = _escribir(tmp_path, "chico", chico)
    bag = load_dagbag(dags_dir)
    problemas = validate(bag, dag_id="prueba", min_tasks=5)
    assert any("al menos 5" in problema for problema in problemas), problemas


def test_un_dag_con_catchup_se_reporta(tmp_path):
    codigo = VALIDO.replace("catchup=False", "catchup=True")
    dags_dir = _escribir(tmp_path, "catchup", codigo)
    bag = load_dagbag(dags_dir)
    problemas = validate(bag, dag_id="prueba")
    assert any("catchup" in problema for problema in problemas), problemas


def test_un_dag_sin_retries_se_reporta(tmp_path):
    codigo = VALIDO.replace(
        'default_args={"retries": 2, "retry_delay": timedelta(minutes=1)}',
        'default_args={"retry_delay": timedelta(minutes=1)}',
    )
    dags_dir = _escribir(tmp_path, "sin_retries", codigo)
    bag = load_dagbag(dags_dir)
    problemas = validate(bag, dag_id="prueba")
    assert any("retries" in problema for problema in problemas), problemas


def test_un_dag_con_schedule_se_reporta(tmp_path):
    codigo = VALIDO.replace("schedule=None", 'schedule="@daily"')
    dags_dir = _escribir(tmp_path, "con_schedule", codigo)
    bag = load_dagbag(dags_dir)
    problemas = validate(bag, dag_id="prueba")
    assert any("schedule" in problema for problema in problemas), problemas
