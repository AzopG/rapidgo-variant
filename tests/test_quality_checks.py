"""Pruebas de `quality.checks` (Issue #16) con tablas en memoria: sin Parquet ni Spark."""

from __future__ import annotations

import ast
from datetime import datetime
from pathlib import Path

import pytest

from quality import checks as qc
from quality.lake import Table

REPO = Path(__file__).resolve().parents[1]
TS = datetime(2026, 11, 4, 12, 0)


# ------------------------------------------------------------------ fixtures


def event(event_id: str, **changes) -> dict:
    row = {
        "event_id": event_id,
        "order_id": f"ord-{event_id}",
        "zone_id": "Z-NORTE",
        "channel": "app",
        "rating": 4,
        "occurred_at": "2026-11-04T12:00:00.000Z",
        "event_ts": TS,
        "dt": "2026-11-04",
    }
    row.update(changes)
    return row


def table(rows: list[dict], columns: list[str] | None = None) -> Table:
    if columns is None:
        columns = list(rows[0]) if rows else []
    return Table(columns=columns, rows=rows)


def raw_tables(n: int = 3):
    rows = [event(f"e{i}") for i in range(n)]
    rejected = table(
        [{"event_id": "bad-1", "rejection_reason": "rating_out_of_range"}]
    )
    return table(rows), table(list(rows)), rejected


def k1(value=4.1, n=25, **changes) -> dict:
    row = {
        "window_start": TS,
        "window_end": TS,
        "dimension": "global",
        "dimension_value": "GLOBAL",
        "value": value,
        "n": n,
    }
    row.update(changes)
    return row


def k2(share=0.4, n=8, rank=1, reason="late_delivery") -> dict:
    return {
        "window_start": TS,
        "window_end": TS,
        "complaint_reason": reason,
        "n": n,
        "total_reclamos": 20,
        "share": share,
        "rank": rank,
    }


def k3(avg_now=3.9, avg_previous=4.0, delta="auto", state="estable") -> dict:
    if delta == "auto":
        delta = None if avg_now is None or avg_previous is None else avg_now - avg_previous
    return {
        "window_start": TS,
        "window_end": TS,
        "avg_now": avg_now,
        "n_now": 100,
        "avg_previous": avg_previous,
        "n_previous": 100,
        "delta": delta,
        "pct_delta": None,
        "state": state,
    }


def alerts_table(n: int = 2) -> Table:
    rows = [{column: None for column in qc.ALERT_COLUMNS} for _ in range(n)]
    return Table(columns=list(qc.ALERT_COLUMNS), rows=rows)


def curated_tables(**overrides):
    kpis = {
        "kpi_rating_avg": table([k1(), k1(dimension="zone_id", dimension_value="Z-SUR")]),
        "kpi_top_reasons": table([k2(), k2(share=0.3, rank=2, reason="cold_food")]),
        "kpi_trend": table([k3()]),
    }
    kpis.update(overrides)
    return kpis


def by_name(result: dict) -> dict:
    return {item["name"]: item for item in result["checks"]}


# ------------------------------------------------------------- shape del contrato


def test_shape_matches_the_contract():
    result = qc.evaluate_raw(*raw_tables())
    assert set(result) == {"ok", "checks", "metrics"}
    assert isinstance(result["ok"], bool)
    for item in result["checks"]:
        assert set(item) == {"name", "ok", "detail"}
        assert isinstance(item["detail"], str)


def test_ok_is_false_if_any_check_fails_but_never_raises():
    raw, processed, rejected = raw_tables()
    result = qc.evaluate_raw(None, processed, rejected)
    assert result["ok"] is False
    assert by_name(result)["raw_no_vacio"]["ok"] is False


# --------------------------------------------------------------------- check_raw


def test_healthy_raw_passes():
    result = qc.evaluate_raw(*raw_tables(5))
    assert result["ok"] is True, result
    assert result["metrics"] == {
        "raw_rows": 5,
        "processed_rows": 5,
        "rejected_rows": 1,
        "duplicated_event_ids": 0,
    }


@pytest.mark.parametrize("missing", ["raw", "processed"])
def test_empty_layers_fail(missing):
    raw, processed, rejected = raw_tables()
    empty = Table(columns=raw.columns, rows=[])
    args = {"raw": raw, "processed": processed, "rejected": rejected, missing: empty}
    result = qc.evaluate_raw(args["raw"], args["processed"], args["rejected"])
    assert by_name(result)[f"{missing}_no_vacio"]["ok"] is False
    assert "no tiene filas" in by_name(result)[f"{missing}_no_vacio"]["detail"]


@pytest.mark.parametrize("column", qc.PROCESSED_REQUIRED_COLUMNS)
def test_each_missing_contract_column_is_reported(column):
    raw, processed, rejected = raw_tables()
    rows = [{k: v for k, v in row.items() if k != column} for row in processed.rows]
    broken = table(rows, [c for c in processed.columns if c != column])
    item = by_name(qc.evaluate_raw(raw, broken, rejected))["processed_esquema"]
    assert item["ok"] is False
    assert column in item["detail"]


def test_duplicated_event_id_fails_and_is_counted():
    raw, _, rejected = raw_tables()
    processed = table([event("e1"), event("e2"), event("e1"), event("e2"), event("e3")])
    result = qc.evaluate_raw(raw, processed, rejected)
    item = by_name(result)["processed_sin_duplicados"]
    assert item["ok"] is False
    assert "2 event_id duplicados: e1, e2" in item["detail"]
    assert result["metrics"]["duplicated_event_ids"] == 2


def test_processed_cannot_have_more_rows_than_raw():
    raw = table([event("e1")])
    processed = table([event("e1"), event("e2")])
    item = by_name(qc.evaluate_raw(raw, processed, None))["processed_no_supera_raw"]
    assert item["ok"] is False
    assert "processed=2 raw=1" in item["detail"]


def test_processed_equal_to_raw_passes():
    raw, processed, _ = raw_tables(4)
    assert by_name(qc.evaluate_raw(raw, processed, None))["processed_no_supera_raw"]["ok"]


@pytest.mark.parametrize("reason", [None, ""])
def test_rejected_without_reason_fails(reason):
    raw, processed, _ = raw_tables()
    rejected = table(
        [
            {"event_id": "bad-1", "rejection_reason": "invalid_channel"},
            {"event_id": "bad-2", "rejection_reason": reason},
        ]
    )
    item = by_name(qc.evaluate_raw(raw, processed, rejected))["rechazados_con_motivo"]
    assert item["ok"] is False
    assert "1 de 2" in item["detail"] and "bad-2" in item["detail"]


def test_run_without_rejections_passes_the_reason_check():
    raw, processed, _ = raw_tables()
    item = by_name(qc.evaluate_raw(raw, processed, None))["rechazados_con_motivo"]
    assert item["ok"] is True
    assert item["detail"] == "sin rechazados en la corrida"


def test_examples_are_capped_in_the_detail():
    raw = table([event(f"e{i}") for i in range(20)])
    processed = table([event(f"e{i}") for i in range(5)] * 2)
    detail = by_name(qc.evaluate_raw(raw, processed, None))["processed_sin_duplicados"]["detail"]
    assert "(+2 mas)" in detail


# ----------------------------------------------------------------- check_curated


def test_healthy_curated_passes():
    result = qc.evaluate_curated(curated_tables(), alerts_table(3))
    assert result["ok"] is True, result
    assert result["metrics"]["alerts_rows"] == 3
    assert result["metrics"]["k3_state"] == "estable"
    assert result["metrics"]["kpi_rating_avg_rows"] == 2


@pytest.mark.parametrize("name", qc.KPI_TABLES)
def test_missing_kpi_table_fails(name):
    result = qc.evaluate_curated(curated_tables(**{name: None}), alerts_table())
    assert by_name(result)[f"{name}_no_vacio"]["ok"] is False
    assert result["ok"] is False


@pytest.mark.parametrize(
    "row, ok",
    [
        (k1(value=1.0), True),
        (k1(value=5.0), True),
        (k1(value=None, n=0), True),
        (k1(value=0.99), False),
        (k1(value=5.01), False),
        (k1(value=None, n=3), False),
        (k1(n=-1), False),
    ],
)
def test_k1_value_range(row, ok):
    assert qc.rating_avg_in_range(table([row]))["ok"] is ok


@pytest.mark.parametrize(
    "row, ok",
    [
        (k2(share=0.0), True),
        (k2(share=1.0), True),
        (k2(rank=5), True),
        (k2(share=1.2), False),
        (k2(share=-0.1), False),
        (k2(n=0), False),
        (k2(rank=0), False),
        (k2(rank=6), False),
    ],
)
def test_k2_ranges(row, ok):
    assert qc.top_reasons_in_range(table([row]))["ok"] is ok


@pytest.mark.parametrize(
    "row, ok",
    [
        (k3(), True),
        (k3(state="deterioro", avg_now=3.5), True),
        (k3(avg_previous=None, state=None), True),
        (k3(state="empeorando"), False),
        (k3(delta=0.5), False),
        (k3(delta=None), False),
        (k3(avg_previous=None, delta=0.1), False),
        (k3(avg_previous=None, state="estable"), False),
    ],
)
def test_k3_state_and_delta(row, ok):
    assert qc.trend_consistent(table([row]))["ok"] is ok


def test_alerts_must_exist():
    item = by_name(qc.evaluate_curated(curated_tables(), None))["alerts_esquema"]
    assert item["ok"] is False
    assert "no existe curated/alerts" in item["detail"]


def test_alerts_missing_column_is_named():
    columns = [c for c in qc.ALERT_COLUMNS if c != "threshold"]
    item = qc.alerts_schema(Table(columns=columns, rows=[]))
    assert item["ok"] is False
    assert "threshold" in item["detail"]


def test_alert_columns_match_alert_schema_in_the_spark_job():
    """Si alguien cambia ALERT_SCHEMA, este test obliga a actualizar la calidad."""
    source = (REPO / "spark" / "alerts_job.py").read_text(encoding="utf-8-sig")
    names = [
        node.args[0].value
        for node in ast.walk(ast.parse(source))
        if isinstance(node, ast.Call)
        and getattr(node.func, "attr", None) == "StructField"
        and node.args
        and isinstance(node.args[0], ast.Constant)
    ]
    assert tuple(names) == qc.ALERT_COLUMNS


def test_trend_states_match_kpi_rules():
    from spark import kpi_rules

    assert qc.TREND_STATES == {
        kpi_rules.TREND_STABLE,
        kpi_rules.TREND_DOWN,
        kpi_rules.TREND_UP,
    }


# ---------------------------------------------------------- API que llama el DAG


def fake_reader(tables: dict[str, Table | None]):
    def reader(path: Path):
        key = Path(path).relative_to(LAKE).as_posix()
        return tables.get(key)

    return reader


LAKE = Path("/lake/runs/20261104T120000Z")


def test_check_raw_reads_the_three_layers_of_the_run():
    raw, processed, rejected = raw_tables()
    reader = fake_reader(
        {"raw": raw, "processed": processed, "curated/ratings_rejected": rejected}
    )
    assert qc.check_raw(str(LAKE), reader=reader)["ok"] is True


def test_check_curated_reads_kpis_and_alerts():
    tables = {f"curated/{name}": t for name, t in curated_tables().items()}
    tables["curated/alerts"] = alerts_table()
    assert qc.check_curated(str(LAKE), reader=fake_reader(tables))["ok"] is True


def test_dag_signatures_are_the_contract():
    import inspect

    assert list(inspect.signature(qc.check_raw).parameters)[0] == "lake_dir"
    assert list(inspect.signature(qc.check_curated).parameters)[0] == "lake_dir"
