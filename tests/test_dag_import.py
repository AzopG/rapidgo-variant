"""El DAG real importa y cumple el contrato (Issue #18).

Estas pruebas necesitan el paquete `apache-airflow`, que la suite principal no
instala: corren en el job `dag` del CI, que lo instala pinneado. En el resto de los
entornos se saltean, y el paso `python -m scripts.validate_dag` es el que hace de
guarda en cualquier caso.
"""

from __future__ import annotations

import pytest

# `airflow.models` y no `airflow`: el directorio `airflow/` del repo es un paquete de
# espacio de nombres, asi que `import airflow` funciona aunque el paquete no exista.
pytest.importorskip(
    "airflow.models",
    reason="apache-airflow solo se instala en el job `dag` del CI",
)

from scripts.validate_dag import DAG_ID, REPO_ROOT, load_dagbag, validate  # noqa: E402

DAGS_DIR = REPO_ROOT / "airflow" / "dags"

# Las tasks que el plan de la semana 3 pide: ingesta, alertas y agregacion.
CORE_TASKS = {
    "iniciar_corrida",
    "crear_topic",
    "generar_eventos",
    "ejecutar_ingesta",
    "ejecutar_alertas",
    "calcular_kpis",
}


@pytest.fixture(scope="module")
def bag():
    return load_dagbag(DAGS_DIR)


def test_el_dag_importa_sin_errores(bag):
    assert bag.import_errors == {}


def test_el_dag_cumple_el_contrato(bag):
    assert validate(bag) == []


def test_el_dag_declara_las_tasks_del_pipeline(bag):
    assert DAG_ID in bag.dags
    faltantes = CORE_TASKS - set(bag.dags[DAG_ID].task_ids)
    assert not faltantes, f"faltan tasks en el DAG: {sorted(faltantes)}"


def test_el_comando_de_verificacion_termina_en_cero(capsys):
    # El job de CI no inicializa la base de Airflow: si este comando la necesitara,
    # fallaria con "You need to initialize the database" antes de mirar el DAG.
    from scripts.validate_dag import EXIT_OK, main

    assert main([]) == EXIT_OK
    assert "DAG_OK" in capsys.readouterr().out
