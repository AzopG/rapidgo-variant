"""Reglas de las alertas A1 y A2 (`docs/kpis-alerts.md:28-47`).

Son funciones puras y sin Spark a proposito: los bordes de umbral son lo que hay que
poder probar sin levantar un contenedor, y son los mismos casos que pide el Issue #12.

Los umbrales se comparan con el valor tal cual, sin epsilon: `3.60` no dispara A1 y
`0.30` si dispara A2, como fija `docs/kpis-alerts.md` y el plan de la semana.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta

A1_ID = "A1"
A2_ID = "A2"

A1_WINDOW = "10 minutes"
A2_WINDOW = "15 minutes"
COOLDOWN = timedelta(minutes=15)

A1_WARNING_BELOW = 3.60
A1_CRITICAL_BELOW = 3.20
A1_MIN_EVENTS = 20

A2_WARNING_SHARE = 0.30
A2_CRITICAL_SHARE = 0.50
A2_MIN_COMPLAINTS = 10

GLOBAL_ZONE = "GLOBAL"

SEVERITY_WARNING = "warning"
SEVERITY_CRITICAL = "critical"


@dataclass(frozen=True)
class Decision:
    """Severidad elegida y umbral que la disparo."""

    alert_id: str
    severity: str
    threshold: float


def a1_severity(avg_rating: float | None, n: int | None) -> Decision | None:
    """A1: caida de calificacion sobre la ventana de 10 minutos."""
    if avg_rating is None or n is None or n < A1_MIN_EVENTS:
        return None
    if avg_rating < A1_CRITICAL_BELOW:
        return Decision(A1_ID, SEVERITY_CRITICAL, A1_CRITICAL_BELOW)
    if avg_rating < A1_WARNING_BELOW:
        return Decision(A1_ID, SEVERITY_WARNING, A1_WARNING_BELOW)
    return None


def a2_severity(share: float | None, n_reclamos: int | None) -> Decision | None:
    """A2: concentracion de reclamos en un motivo sobre la ventana de 15 minutos."""
    if share is None or n_reclamos is None or n_reclamos < A2_MIN_COMPLAINTS:
        return None
    if share >= A2_CRITICAL_SHARE:
        return Decision(A2_ID, SEVERITY_CRITICAL, A2_CRITICAL_SHARE)
    if share >= A2_WARNING_SHARE:
        return Decision(A2_ID, SEVERITY_WARNING, A2_WARNING_SHARE)
    return None


def cooldown_key(alert_id: str, zone_id: str | None, complaint_reason: str | None) -> tuple:
    """Clave del cooldown: (alerta, zona, motivo). A1 no usa motivo."""
    return alert_id, zone_id, complaint_reason


def cooldown_allows(
    key: tuple,
    moment: datetime,
    last_emitted: dict[tuple, datetime],
    cooldown: timedelta = COOLDOWN,
) -> bool:
    """True si la clave puede volver a alertar: nunca sono o paso el cooldown."""
    previous = last_emitted.get(key)
    if previous is None:
        return True
    return moment - previous >= cooldown
