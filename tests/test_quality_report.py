"""Pruebas de `quality.report` (Issue #16): shape del reporte y escritura del archivo."""

from __future__ import annotations

import json
from datetime import date, datetime, timezone
from pathlib import Path

from quality import report as qr
from quality.lake import Table

NOW = datetime(2026, 11, 4, 12, 30, 0, 123456, tzinfo=timezone.utc)
TS = datetime(2026, 11, 4, 11, 0)


def t(rows):
    return Table(columns=list(rows[0]) if rows else [], rows=rows)


def lake_tables():
    return {
        "curated/kpi_rating_avg": t(
            [
                {"window_start": datetime(2026, 11, 4, 11, 1), "dimension": "global",
                 "dimension_value": "GLOBAL", "value": 3.9, "n": 30},
                {"window_start": TS, "dimension": "zone_id",
                 "dimension_value": "Z-SUR", "value": 4.2, "n": 12},
            ]
        ),
        "curated/kpi_top_reasons": t(
            [
                {"window_start": TS, "complaint_reason": "cold_food", "share": 0.3, "rank": 2, "n": 6},
                {"window_start": TS, "complaint_reason": "late_delivery", "share": 0.5, "rank": 1, "n": 10},
            ]
        ),
        "curated/kpi_trend": t(
            [{"avg_now": 3.8, "avg_previous": 4.1, "delta": -0.3, "state": "deterioro"}]
        ),
        "curated/alerts": t(
            [{"alert_id": "A1", "severity": "critical", "window_start": TS,
              "zone_id": "GLOBAL", "complaint_reason": None,
              "observed_value": 3.1, "threshold": 3.2}]
        ),
    }


def reader_for(lake: Path, tables):
    def reader(path: Path):
        return tables.get(Path(path).relative_to(lake).as_posix())

    return reader


QUALITY = {
    "raw": {"ok": True, "checks": [], "metrics": {"raw_rows": 300}},
    "curated": {"ok": False, "checks": [{"name": "x", "ok": False, "detail": "y"}], "metrics": {}},
}


def build(tmp_path, tables=None, quality=QUALITY):
    lake = tmp_path / "runs" / "20261104T120000Z"
    lake.mkdir(parents=True)
    path = qr.build_report(
        str(lake), quality, reader=reader_for(lake, tables or lake_tables()), now=NOW
    )
    return lake, path, json.loads(Path(path).read_text(encoding="utf-8"))


def test_report_is_written_where_the_contract_says(tmp_path):
    lake, path, _ = build(tmp_path)
    assert path == str(lake / "curated" / "report" / "report.json")
    assert isinstance(path, str)


def test_report_has_the_contract_shape(tmp_path):
    _, _, content = build(tmp_path)
    assert set(content) == {"run_id", "generated_at", "kpis", "alerts", "quality"}
    assert set(content["kpis"]) == {"kpi_rating_avg", "kpi_top_reasons", "kpi_trend"}
    assert set(content["quality"]) == {"raw", "curated"}


def test_run_id_and_generated_at(tmp_path):
    _, _, content = build(tmp_path)
    assert content["run_id"] == "20261104T120000Z"
    assert content["generated_at"] == "2026-11-04T12:30:00Z"


def test_three_kpis_and_alerts_are_in_the_report(tmp_path):
    _, _, content = build(tmp_path)
    assert len(content["kpis"]["kpi_rating_avg"]) == 2
    assert [r["rank"] for r in content["kpis"]["kpi_top_reasons"]] == [1, 2]
    assert content["kpis"]["kpi_trend"]["state"] == "deterioro"
    assert content["alerts"][0]["severity"] == "critical"


def test_timestamps_are_utc_iso(tmp_path):
    _, _, content = build(tmp_path)
    assert content["alerts"][0]["window_start"] == "2026-11-04T11:00:00Z"


def test_rows_are_sorted_so_reports_are_reproducible(tmp_path):
    _, _, content = build(tmp_path)
    starts = [r["window_start"] for r in content["kpis"]["kpi_rating_avg"]]
    assert starts == sorted(starts)


def test_quality_results_are_copied_as_received(tmp_path):
    _, _, content = build(tmp_path)
    assert content["quality"] == QUALITY


def test_missing_tables_give_empty_sections_not_an_error(tmp_path):
    _, _, content = build(tmp_path, tables={"curated/none": None}, quality={})
    assert content["kpis"] == {"kpi_rating_avg": [], "kpi_top_reasons": [], "kpi_trend": None}
    assert content["alerts"] == []
    assert content["quality"] == {"raw": None, "curated": None}


def test_json_conversion_handles_spark_types():
    assert qr.to_json_value(float("nan")) is None
    assert qr.to_json_value(date(2026, 11, 4)) == "2026-11-04"
    assert qr.to_json_value({"a": [datetime(2026, 1, 1)]}) == {"a": ["2026-01-01T00:00:00Z"]}


def test_sort_tolerates_nulls():
    rows = [{"window_start": None, "rank": 1}, {"window_start": TS, "rank": 2}]
    ordered = qr.rows(t(rows), qr.K2_ORDER)
    assert ordered[-1]["window_start"] is None
