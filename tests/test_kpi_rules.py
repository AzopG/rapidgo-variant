"""Tests de las reglas puras de K1/K2/K3 (Issue #15)."""

from __future__ import annotations

import pytest

from spark.kpi_rules import (
    TOP_REASONS,
    TREND_DOWN,
    TREND_STABLE,
    TREND_UP,
    delta,
    percent_delta,
    reason_shares,
    trend_state,
)


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (-0.21, TREND_DOWN),
        (-0.20, TREND_DOWN),  # borde inclusivo, como la tabla documentada
        (-0.19, TREND_STABLE),
        (0.0, TREND_STABLE),
        (0.19, TREND_STABLE),
        (0.20, TREND_UP),
        (0.21, TREND_UP),
    ],
)
def test_estado_de_tendencia_en_los_bordes(value, expected):
    assert trend_state(value) == expected


def test_tendencia_sin_datos_no_es_estable():
    assert trend_state(None) is None


def test_delta_absoluto_y_porcentual():
    assert delta(4.2, 4.0) == pytest.approx(0.2)
    assert percent_delta(4.2, 4.0) == pytest.approx(5.0)


def test_delta_sin_base_no_inventa_numero():
    assert delta(4.2, None) is None
    assert percent_delta(None, 4.0) is None
    assert percent_delta(4.0, 0.0) is None


def test_top_de_motivos_ordena_por_conteo_y_desempata_por_nombre():
    counts = {"late_delivery": 5, "cold_food": 5, "other": 1}
    shares = reason_shares(counts, limit=2)
    assert [reason for reason, _, _ in shares] == ["cold_food", "late_delivery"]
    assert shares[0][1] == 5
    assert shares[0][2] == pytest.approx(5 / 11)


def test_top_de_motivos_recorta_al_limite_documentado():
    counts = {f"motivo_{index}": index + 1 for index in range(8)}
    shares = reason_shares(counts)
    assert len(shares) == TOP_REASONS
    assert shares[0][0] == "motivo_7"


def test_top_de_motivos_sin_reclamos_devuelve_lista_vacia():
    assert reason_shares({}) == []
    assert reason_shares({"late_delivery": 0}) == []
