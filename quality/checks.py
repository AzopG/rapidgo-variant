"""Comprobaciones de calidad sobre el lake de una corrida (Issue #16).

Contrato con el DAG (comentario de Ivan en #16):

    check_raw(lake_dir: str) -> dict
    check_curated(lake_dir: str) -> dict

Las dos devuelven el mismo shape:

    {"ok": bool, "checks": [{"name", "ok", "detail"}], "metrics": {...}}

`ok=False` no lanza: la task termina, el reporte deja constancia y el pipeline sigue.
Un error de lectura si lanza, para que Airflow reintente.

Cada comprobacion es una funcion pura sobre `Table`; `check_raw` y `check_curated` solo
leen el lake y las arman. Asi las reglas se prueban sin Parquet ni Spark.
"""

from __future__ import annotations

import math
from pathlib import Path
from typing import Any, Iterable, Mapping

from quality.lake import Reader, Table, read_table

# Esquema minimo de `processed` segun el contrato de #16.
PROCESSED_REQUIRED_COLUMNS: tuple[str, ...] = (
    "event_id",
    "order_id",
    "zone_id",
    "channel",
    "rating",
    "occurred_at",
    "event_ts",
    "dt",
)

KPI_TABLES: tuple[str, ...] = ("kpi_rating_avg", "kpi_top_reasons", "kpi_trend")

# Mismos campos y orden que `ALERT_SCHEMA` en `spark/alerts_job.py`. No se importa de
# alli porque ese modulo importa pyspark; `tests/test_quality_checks.py` verifica que
# los dos sigan iguales.
ALERT_COLUMNS: tuple[str, ...] = (
    "alert_id",
    "severity",
    "rule_window",
    "window_start",
    "window_end",
    "zone_id",
    "complaint_reason",
    "observed_value",
    "threshold",
    "sample_size",
    "evaluated_at",
)

RATING_MIN = 1.0
RATING_MAX = 5.0
TOP_REASONS = 5
# Mismos estados que `spark/kpi_rules.py` (TREND_STABLE, TREND_DOWN, TREND_UP).
TREND_STATES = frozenset({"estable", "deterioro", "mejora"})
DELTA_TOLERANCE = 1e-6
# Cuantos ejemplos de filas malas se citan en el `detail`, para que el log sea legible.
MAX_EXAMPLES = 3


# ----------------------------------------------------------------------- utilidades


def check(name: str, ok: bool, detail: str) -> dict[str, Any]:
    return {"name": name, "ok": bool(ok), "detail": detail}


def result(checks: list[dict[str, Any]], metrics: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "ok": all(item["ok"] for item in checks),
        "checks": checks,
        "metrics": dict(metrics),
    }


def _is_number(value: Any) -> bool:
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and not (isinstance(value, float) and math.isnan(value))
    )


def _examples(items: Iterable[Any]) -> str:
    items = list(items)
    shown = ", ".join(str(item) for item in items[:MAX_EXAMPLES])
    extra = len(items) - MAX_EXAMPLES
    return shown + (f" (+{extra} mas)" if extra > 0 else "")


def _present(name: str, table: Table | None) -> dict[str, Any]:
    """La tabla existe y tiene filas."""
    if table is None:
        return check(f"{name}_no_vacio", False, f"no existe {name}/")
    if len(table) == 0:
        return check(f"{name}_no_vacio", False, f"{name}/ existe pero no tiene filas")
    return check(f"{name}_no_vacio", True, f"{len(table)} filas")


# ------------------------------------------------------------------- reglas de raw


def processed_schema(table: Table | None) -> dict[str, Any]:
    if table is None:
        return check("processed_esquema", False, "no existe processed/")
    missing = [name for name in PROCESSED_REQUIRED_COLUMNS if name not in table.columns]
    if missing:
        return check(
            "processed_esquema", False, f"faltan columnas: {', '.join(missing)}"
        )
    return check(
        "processed_esquema",
        True,
        f"{len(PROCESSED_REQUIRED_COLUMNS)} columnas del contrato presentes",
    )


def duplicated_event_ids(table: Table | None) -> list[str]:
    """`event_id` que aparecen mas de una vez, en orden de primera aparicion."""
    if table is None or "event_id" not in table.columns:
        return []
    seen: set[Any] = set()
    duplicated: dict[Any, None] = {}
    for row in table.rows:
        event_id = row.get("event_id")
        if event_id in seen:
            duplicated[event_id] = None
        seen.add(event_id)
    return [str(event_id) for event_id in duplicated]


def processed_unique(table: Table | None) -> dict[str, Any]:
    if table is None or "event_id" not in table.columns:
        return check("processed_sin_duplicados", False, "processed/ no tiene event_id")
    duplicated = duplicated_event_ids(table)
    if duplicated:
        return check(
            "processed_sin_duplicados",
            False,
            f"{len(duplicated)} event_id duplicados: {_examples(duplicated)}",
        )
    return check("processed_sin_duplicados", True, f"{len(table)} event_id unicos")


def processed_not_above_raw(raw: Table | None, processed: Table | None) -> dict[str, Any]:
    raw_rows = len(raw) if raw is not None else 0
    processed_rows = len(processed) if processed is not None else 0
    ok = processed_rows <= raw_rows
    detail = f"processed={processed_rows} raw={raw_rows}"
    if not ok:
        detail += ": la deduplicacion solo puede quitar filas"
    return check("processed_no_supera_raw", ok, detail)


def rejected_have_reason(table: Table | None) -> dict[str, Any]:
    """Todo rechazado trae su motivo. Sin rechazados en la corrida no hay nada que validar."""
    name = "rechazados_con_motivo"
    if table is None or len(table) == 0:
        return check(name, True, "sin rechazados en la corrida")
    if "rejection_reason" not in table.columns:
        return check(name, False, "ratings_rejected/ no tiene la columna rejection_reason")
    without = [
        row.get("event_id")
        for row in table.rows
        if row.get("rejection_reason") in (None, "")
    ]
    if without:
        return check(
            name,
            False,
            f"{len(without)} de {len(table)} rechazados sin rejection_reason: "
            f"{_examples(without)}",
        )
    return check(name, True, f"{len(table)} rechazados, todos con motivo")


def evaluate_raw(
    raw: Table | None, processed: Table | None, rejected: Table | None
) -> dict[str, Any]:
    checks = [
        _present("raw", raw),
        _present("processed", processed),
        processed_schema(processed),
        processed_unique(processed),
        processed_not_above_raw(raw, processed),
        rejected_have_reason(rejected),
    ]
    metrics = {
        "raw_rows": len(raw) if raw is not None else 0,
        "processed_rows": len(processed) if processed is not None else 0,
        "rejected_rows": len(rejected) if rejected is not None else 0,
        "duplicated_event_ids": len(duplicated_event_ids(processed)),
    }
    return result(checks, metrics)


# --------------------------------------------------------------- reglas de curated


def rating_avg_in_range(table: Table | None) -> dict[str, Any]:
    """K1: `value` en [1, 5], o null solo cuando `n=0`; `n >= 0`."""
    name = "kpi_rating_avg_en_rango"
    if table is None or len(table) == 0:
        return check(name, False, "kpi_rating_avg/ sin filas")
    bad: list[str] = []
    for row in table.rows:
        value, n = row.get("value"), row.get("n")
        label = f"{row.get('dimension')}={row.get('dimension_value')}"
        if not _is_number(n) or n < 0:
            bad.append(f"{label} n={n}")
        elif value is None:
            if n != 0:
                bad.append(f"{label} value=null con n={n}")
        elif not _is_number(value) or not RATING_MIN <= value <= RATING_MAX:
            bad.append(f"{label} value={value}")
    if bad:
        return check(name, False, f"{len(bad)} filas fuera de rango: {_examples(bad)}")
    return check(name, True, f"{len(table)} filas con value en [1, 5] y n >= 0")


def top_reasons_in_range(table: Table | None) -> dict[str, Any]:
    """K2: `share` en [0, 1], `n > 0`, `rank` entre 1 y 5."""
    name = "kpi_top_reasons_en_rango"
    if table is None or len(table) == 0:
        return check(name, False, "kpi_top_reasons/ sin filas")
    bad: list[str] = []
    for row in table.rows:
        share, n, rank = row.get("share"), row.get("n"), row.get("rank")
        label = str(row.get("complaint_reason"))
        if not _is_number(share) or not 0.0 <= share <= 1.0:
            bad.append(f"{label} share={share}")
        elif not _is_number(n) or n <= 0:
            bad.append(f"{label} n={n}")
        elif not _is_number(rank) or not 1 <= rank <= TOP_REASONS:
            bad.append(f"{label} rank={rank}")
    if bad:
        return check(name, False, f"{len(bad)} filas fuera de rango: {_examples(bad)}")
    return check(name, True, f"{len(table)} filas con share en [0, 1] y rank 1..5")


def trend_consistent(table: Table | None) -> dict[str, Any]:
    """K3: `state` valido o null, y `delta == avg_now - avg_previous`."""
    name = "kpi_trend_coherente"
    if table is None or len(table) == 0:
        return check(name, False, "kpi_trend/ sin filas")
    bad: list[str] = []
    for row in table.rows:
        state = row.get("state")
        now, previous, delta = row.get("avg_now"), row.get("avg_previous"), row.get("delta")
        if state is not None and state not in TREND_STATES:
            bad.append(f"state={state!r}")
        if now is None or previous is None:
            if delta is not None:
                bad.append(f"delta={delta} sin una de las dos medias")
        elif delta is None:
            bad.append("delta=null con las dos medias presentes")
        elif not math.isclose(delta, now - previous, abs_tol=DELTA_TOLERANCE):
            bad.append(f"delta={delta} pero avg_now - avg_previous={now - previous}")
        if delta is None and state is not None:
            bad.append(f"state={state!r} sin delta")
    if bad:
        return check(name, False, f"incoherente: {_examples(bad)}")
    first = table.rows[0]
    return check(
        name,
        True,
        f"state={first.get('state')} delta={first.get('delta')}",
    )


def alerts_schema(table: Table | None) -> dict[str, Any]:
    name = "alerts_esquema"
    if table is None:
        return check(name, False, "no existe curated/alerts/")
    missing = [column for column in ALERT_COLUMNS if column not in table.columns]
    if missing:
        return check(name, False, f"faltan columnas de ALERT_SCHEMA: {', '.join(missing)}")
    return check(name, True, f"{len(table)} alertas con el esquema de ALERT_SCHEMA")


def evaluate_curated(
    kpis: Mapping[str, Table | None], alerts: Table | None
) -> dict[str, Any]:
    checks = [_present(name, kpis.get(name)) for name in KPI_TABLES]
    checks += [
        rating_avg_in_range(kpis.get("kpi_rating_avg")),
        top_reasons_in_range(kpis.get("kpi_top_reasons")),
        trend_consistent(kpis.get("kpi_trend")),
        alerts_schema(alerts),
    ]
    trend = kpis.get("kpi_trend")
    trend_row = trend.rows[0] if trend is not None and len(trend) else {}
    metrics: dict[str, Any] = {
        f"{name}_rows": len(kpis[name]) if kpis.get(name) is not None else 0
        for name in KPI_TABLES
    }
    metrics.update(
        {
            "alerts_rows": len(alerts) if alerts is not None else 0,
            "k3_state": trend_row.get("state"),
            "k3_delta": trend_row.get("delta"),
        }
    )
    return result(checks, metrics)


# --------------------------------------------------------------- API para el DAG


def check_raw(lake_dir: str, reader: Reader = read_table) -> dict:
    """Valida `raw/` y `processed/` (y los rechazados) de la corrida."""
    lake = Path(lake_dir)
    return evaluate_raw(
        reader(lake / "raw"),
        reader(lake / "processed"),
        reader(lake / "curated" / "ratings_rejected"),
    )


def check_curated(lake_dir: str, reader: Reader = read_table) -> dict:
    """Valida los tres KPIs y las alertas en `curated/`."""
    curated = Path(lake_dir) / "curated"
    kpis = {name: reader(curated / name) for name in KPI_TABLES}
    return evaluate_curated(kpis, reader(curated / "alerts"))
