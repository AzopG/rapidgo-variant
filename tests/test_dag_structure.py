"""Estructura del DAG sin importar Airflow (Issue #15).

La validacion real de importacion es de #18 (Andres), que decide como se instala
Airflow en CI. Este test cubre lo que se puede afirmar sin el paquete: que el archivo
compile, que declare al menos cinco tasks, que las encadene y que tenga retries.
"""

from __future__ import annotations

import ast
from pathlib import Path

DAG_PATH = Path(__file__).resolve().parents[1] / "airflow" / "dags" / "rapidgo_pipeline.py"
SOURCE = DAG_PATH.read_text(encoding="utf-8")
TREE = ast.parse(SOURCE)


def _python_operator_task_ids() -> list[str]:
    ids: list[str] = []
    for node in ast.walk(TREE):
        if not isinstance(node, ast.Call):
            continue
        if getattr(node.func, "id", None) != "PythonOperator":
            continue
        for keyword in node.keywords:
            if keyword.arg == "task_id" and isinstance(keyword.value, ast.Constant):
                ids.append(keyword.value.value)
    return ids


def test_el_dag_compila():
    compile(SOURCE, str(DAG_PATH), "exec")


def test_el_dag_declara_al_menos_cinco_tasks():
    ids = _python_operator_task_ids()
    assert len(ids) >= 5, ids
    assert len(set(ids)) == len(ids)


def test_el_dag_encadena_tasks_con_bitshift():
    chains = [
        node
        for node in ast.walk(TREE)
        if isinstance(node, ast.BinOp) and isinstance(node.op, ast.RShift)
    ]
    assert chains, "el DAG no declara dependencias con >>"


def test_el_dag_configura_retries():
    assert '"retries"' in SOURCE or "'retries'" in SOURCE
    assert "retry_delay" in SOURCE


def test_las_tasks_de_calidad_importan_el_contrato_de_luis():
    # El contrato de #16 se importa dentro de la funcion para que el DAG sea
    # importable mientras el modulo no exista todavia.
    assert "from quality.checks import check_raw" in SOURCE
    assert "from quality.checks import check_curated" in SOURCE
    assert "from quality.report import build_report" in SOURCE
