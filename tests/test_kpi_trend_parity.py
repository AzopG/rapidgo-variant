"""Paridad entre la tabla de K3 y el SQL de `k3_frame` (Issue #18).

La tabla de `docs/kpis-alerts.md` vive dos veces: en `spark/kpi_rules.trend_state`, que
es Python puro, y en el `when` de `spark/kpis_job.k3_frame`, que es SQL. La semana 2 ya
dejó una divergencia de ese tipo entre el validador y el job (riesgo 7 de la fase 0), y
esta prueba existe para que no se repita en la tendencia.

Se corre con una sesión de Spark real y pocos datos: cada caso arma la media de las dos
ventanas de K3 con ratings enteros, y compara el `state` que decidió el SQL contra el
que decide la regla pura sobre el mismo `delta`, incluidos los bordes inclusivos
`-0.20` y `0.20`.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

pytest.importorskip("pyspark", reason="la paridad de K3 necesita una sesión de Spark")

from spark.kpi_rules import TREND_DOWN, TREND_STABLE, TREND_UP, trend_state  # noqa: E402
from spark.kpis_job import k3_frame  # noqa: E402

REFERENCE = datetime(2026, 11, 5, 12, 0, tzinfo=timezone.utc)
FOURS = [4] * 10


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession  # noqa: PLC0415

    session = (
        SparkSession.builder.master("local[1]")
        .appName("test-kpi-trend-parity")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def _rows(now_ratings, previous_ratings=FOURS):
    """Ratings repartidos dentro de las dos ventanas de K3, un minuto aparte."""
    rows = []
    previous_start = REFERENCE - timedelta(minutes=120)
    # La ventana es abierta a la izquierda, asi que el primer evento va un minuto adentro.
    for index, rating in enumerate(previous_ratings):
        rows.append((rating, previous_start + timedelta(minutes=index + 1)))
    for index, rating in enumerate(now_ratings):
        rows.append((rating, REFERENCE - timedelta(minutes=len(now_ratings) - index)))
    return rows


def _trend(spark, rows):
    frame = spark.createDataFrame(rows, "rating integer, event_ts timestamp")
    return k3_frame(frame, REFERENCE).first()


@pytest.mark.parametrize(
    ("now_ratings", "expected_delta", "expected_state"),
    [
        (FOURS, 0.0, TREND_STABLE),
        ([4] * 9 + [5], 0.1, TREND_STABLE),
        ([4] * 9 + [3], -0.1, TREND_STABLE),
        ([4] * 8 + [5, 5], 0.2, TREND_UP),
        ([4] * 8 + [3, 3], -0.2, TREND_DOWN),
        ([3] * 10, -1.0, TREND_DOWN),
        ([5] * 10, 1.0, TREND_UP),
    ],
)
def test_el_estado_de_k3_coincide_con_la_tabla_documentada(
    spark, now_ratings, expected_delta, expected_state
):
    row = _trend(spark, _rows(now_ratings))

    assert row.delta == pytest.approx(expected_delta, abs=1e-9)
    assert row.state == expected_state
    # La paridad que importa: el SQL y la regla pura tienen que decidir lo mismo.
    assert row.state == trend_state(row.delta)


def test_los_bordes_de_k3_son_inclusivos_en_el_sql(spark):
    # -0.20 es deterioro y 0.20 es mejora, no estable: la tabla usa <= y >=.
    abajo = _trend(spark, _rows([4] * 8 + [3, 3]))
    arriba = _trend(spark, _rows([4] * 8 + [5, 5]))

    assert abajo.delta <= -0.20 and abajo.state == TREND_DOWN
    assert arriba.delta >= 0.20 and arriba.state == TREND_UP


def test_sin_ventana_previa_la_tendencia_queda_nula(spark):
    row = _trend(spark, _rows(FOURS, previous_ratings=[]))

    assert row.avg_previous is None
    assert row.delta is None
    assert row.state is None
    assert row.state == trend_state(None)
