"""Reporte final de la corrida (Issue #16).

Contrato con el DAG:

    build_report(lake_dir: str, quality: dict) -> str

Escribe `<lake>/curated/report/report.json` con los tres KPIs, las alertas disparadas y
el resultado de calidad que el DAG trae por XCom, y devuelve la ruta del archivo.
"""

from __future__ import annotations

import json
import math
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Mapping

from quality.lake import Reader, Table, read_table

REPORT_DIR = ("curated", "report")
REPORT_FILE = "report.json"

# Orden estable: dos corridas con los mismos datos producen el mismo archivo.
K1_ORDER = ("window_start", "dimension", "dimension_value")
K2_ORDER = ("window_start", "rank")
ALERTS_ORDER = ("window_start", "alert_id", "zone_id", "complaint_reason")


def to_json_value(value: Any) -> Any:
    """Convierte lo que devuelve pyarrow a algo que `json` sabe escribir."""
    if isinstance(value, datetime):
        if value.tzinfo is None:
            # Spark guarda los timestamps en UTC; pyarrow los devuelve sin zona.
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    if isinstance(value, Mapping):
        return {str(key): to_json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [to_json_value(item) for item in value]
    return value


def _sort_key(row: Mapping[str, Any], columns: tuple[str, ...]) -> tuple:
    # (True, valor) deja los null al final y evita comparar None con datetime.
    return tuple(
        (row.get(column) is None, str(row.get(column))) for column in columns
    )


def rows(table: Table | None, order: tuple[str, ...]) -> list[dict[str, Any]]:
    if table is None:
        return []
    ordered = sorted(table.rows, key=lambda row: _sort_key(row, order))
    return [to_json_value(row) for row in ordered]


def run_id_from_lake(lake_dir: str | Path) -> str:
    """El lake de la corrida es `datalake/runs/<run_id>/`: el run_id es la carpeta."""
    return Path(lake_dir).resolve().name


def assemble(
    run_id: str,
    generated_at: datetime,
    kpis: Mapping[str, Table | None],
    alerts: Table | None,
    quality: Mapping[str, Any] | None,
) -> dict[str, Any]:
    """Arma el contenido del reporte con el shape del contrato de #16."""
    trend_rows = rows(kpis.get("kpi_trend"), ())
    quality = dict(quality or {})
    return {
        "run_id": run_id,
        "generated_at": to_json_value(generated_at.replace(microsecond=0)),
        "kpis": {
            "kpi_rating_avg": rows(kpis.get("kpi_rating_avg"), K1_ORDER),
            "kpi_top_reasons": rows(kpis.get("kpi_top_reasons"), K2_ORDER),
            # K3 es una sola fila por corrida; null si el job no la dejo.
            "kpi_trend": trend_rows[0] if trend_rows else None,
        },
        "alerts": rows(alerts, ALERTS_ORDER),
        "quality": {
            "raw": to_json_value(quality.get("raw")),
            "curated": to_json_value(quality.get("curated")),
        },
    }


def report_path(lake_dir: str | Path) -> Path:
    return Path(lake_dir).joinpath(*REPORT_DIR, REPORT_FILE)


def build_report(
    lake_dir: str,
    quality: dict,
    reader: Reader = read_table,
    now: datetime | None = None,
) -> str:
    """Lee KPIs y alertas del lake, escribe el reporte y devuelve su ruta."""
    curated = Path(lake_dir) / "curated"
    kpis = {
        name: reader(curated / name)
        for name in ("kpi_rating_avg", "kpi_top_reasons", "kpi_trend")
    }
    content = assemble(
        run_id=run_id_from_lake(lake_dir),
        generated_at=now or datetime.now(timezone.utc),
        kpis=kpis,
        alerts=reader(curated / "alerts"),
        quality=quality,
    )
    path = report_path(lake_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(content, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    return str(path)
