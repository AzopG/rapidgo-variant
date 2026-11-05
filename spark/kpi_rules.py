"""Reglas puras de los tres KPIs (`docs/kpis-alerts.md:14-22`).

Igual que `spark/alert_rules.py`, no importa Spark: los bordes de la tendencia y el
calculo de participacion se prueban sin levantar una sesion. `spark/kpis_job.py` arma
las expresiones de Spark a partir de estas mismas constantes, para que la definicion
viva en un solo lugar.
"""

from __future__ import annotations

from typing import Mapping, Sequence

# K1 y K2 son deslizantes cada 1 minuto, igual que las alertas que evaluan.
K1_WINDOW = "10 minutes"
K1_SLIDE = "1 minute"
K2_WINDOW = "15 minutes"
K2_SLIDE = "1 minute"

# K3 compara los ultimos 60 minutos contra los 60 anteriores.
K3_WINDOW_MINUTES = 60
K3_PREVIOUS_MINUTES = 60

TOP_REASONS = 5
TREND_THRESHOLD = 0.20

TREND_STABLE = "estable"
TREND_DOWN = "deterioro"
TREND_UP = "mejora"

DIMENSIONS = ("global", "zone_id", "channel", "courier_id")
GLOBAL_LABEL = "GLOBAL"


def trend_state(delta: float | None, threshold: float = TREND_THRESHOLD) -> str | None:
    """Estado de K3 segun el delta de calificacion media (`docs/kpis-alerts.md:20-26`).

    Los bordes son inclusivos hacia el cambio: `-0.20` ya es deterioro y `0.20` ya es
    mejora, porque la tabla documentada usa `<=` y `>=`.
    """
    if delta is None:
        return None
    if delta <= -threshold:
        return TREND_DOWN
    if delta >= threshold:
        return TREND_UP
    return TREND_STABLE


def delta(now: float | None, previous: float | None) -> float | None:
    """Diferencia absoluta entre las dos ventanas de K3; `None` si falta una."""
    if now is None or previous is None:
        return None
    return now - previous


def percent_delta(now: float | None, previous: float | None) -> float | None:
    """Variacion porcentual de K3; `None` si no hay base contra la cual comparar."""
    if now is None or previous is None or previous == 0:
        return None
    return (now - previous) / previous * 100.0


def reason_shares(
    counts: Mapping[str, int], limit: int = TOP_REASONS
) -> list[tuple[str, int, float]]:
    """Top de motivos con su participacion sobre el total de reclamos.

    Ordena por conteo descendente y desempata por motivo ascendente, para que dos
    corridas con los mismos datos produzcan exactamente el mismo top.
    """
    total = sum(counts.values())
    if total <= 0:
        return []
    ordered: Sequence[tuple[str, int]] = sorted(
        counts.items(), key=lambda item: (-item[1], item[0])
    )
    return [(reason, n, n / total) for reason, n in ordered[:limit]]
