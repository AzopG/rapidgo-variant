"""Contrato entre el DAG y los flags que acepta cada job (Issue #15).

El primer intento de corrida fallo porque el DAG le pasaba `--topic` y
`--checkpoint-dir` al job batch de KPIs, que no los declara. Este test fija el
contrato para que no vuelva a pasar.
"""

from __future__ import annotations

import pytest

from spark.kpis_job import parse_args as parse_kpis_args


def test_el_job_de_kpis_acepta_los_flags_que_le_pasa_el_dag():
    args = parse_kpis_args(["--lake-dir", "/opt/rapidgo/datalake/runs/abc"])
    assert args.lake_dir == "/opt/rapidgo/datalake/runs/abc"
    assert args.reference is None


def test_el_job_de_kpis_rechaza_flags_de_streaming():
    with pytest.raises(SystemExit):
        parse_kpis_args(
            [
                "--lake-dir",
                "/opt/rapidgo/datalake/runs/abc",
                "--topic",
                "rapidgo.ratings.run.abc",
            ]
        )
