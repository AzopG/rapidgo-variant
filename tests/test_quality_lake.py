"""Lectura real de Parquet con la estructura que deja Spark (Issue #16).

Necesita pyarrow (esta en `requirements.txt`, asi que corre en CI). Se arma un lake
chico con particiones Hive, `_SUCCESS` y `.crc`, igual que el de una corrida, y se
corren las funciones del DAG de punta a punta sobre el.
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import pytest

pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")

from quality import checks as qc  # noqa: E402
from quality.lake import read_table  # noqa: E402
from quality.report import build_report  # noqa: E402

TS = datetime(2026, 11, 4, 12, 0)


def write(folder: Path, rows: list[dict], name: str = "part-00000.snappy.parquet") -> None:
    folder.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows), folder / name)
    (folder / "_SUCCESS").write_text("")
    (folder / f".{name}.crc").write_bytes(b"crc")


def events(n: int) -> list[dict]:
    return [
        {
            "event_id": f"e{i}",
            "order_id": f"ord-{i}",
            "zone_id": "Z-NORTE",
            "channel": "app",
            "rating": 4,
            "occurred_at": "2026-11-04T12:00:00.000Z",
            "event_ts": TS,
        }
        for i in range(n)
    ]


@pytest.fixture
def lake(tmp_path) -> Path:
    root = tmp_path / "datalake" / "runs" / "20261104T120000Z"
    write(root / "raw" / "dt=2026-11-04" / "hour=12", events(4))
    write(root / "processed" / "dt=2026-11-04", events(4))
    write(root / "curated" / "ratings_rejected",
          [{"event_id": "bad", "rejection_reason": "invalid_channel", "dt": "2026-11-04"}])
    write(root / "curated" / "kpi_rating_avg",
          [{"window_start": TS, "window_end": TS, "dimension": "global",
            "dimension_value": "GLOBAL", "value": 3.9, "n": 4}])
    write(root / "curated" / "kpi_top_reasons",
          [{"window_start": TS, "window_end": TS, "complaint_reason": "late_delivery",
            "n": 3, "total_reclamos": 4, "share": 0.75, "rank": 1}])
    write(root / "curated" / "kpi_trend",
          [{"avg_now": 3.9, "avg_previous": 4.0, "delta": 3.9 - 4.0, "state": "estable"}])
    write(root / "curated" / "alerts",
          [{column: None for column in qc.ALERT_COLUMNS} | {"alert_id": "A2", "severity": "warning"}])
    return root


def test_missing_folder_is_none(tmp_path):
    assert read_table(tmp_path / "no-existe") is None


def test_hive_partitions_become_columns(lake):
    raw = read_table(lake / "raw")
    assert len(raw) == 4
    assert {"dt", "hour"} <= set(raw.columns)
    processed = read_table(lake / "processed")
    assert processed.rows[0]["dt"] == "2026-11-04"


def test_success_and_crc_files_are_ignored(lake):
    assert len(read_table(lake / "curated" / "kpi_trend")) == 1


def test_dag_functions_end_to_end(lake):
    raw = qc.check_raw(str(lake))
    curated = qc.check_curated(str(lake))
    assert raw["ok"] is True, raw
    assert curated["ok"] is True, curated

    path = build_report(str(lake), {"raw": raw, "curated": curated})
    content = json.loads(Path(path).read_text(encoding="utf-8"))
    assert content["run_id"] == "20261104T120000Z"
    assert content["kpis"]["kpi_trend"]["state"] == "estable"
    assert content["alerts"][0]["alert_id"] == "A2"
    assert content["quality"]["raw"]["metrics"]["raw_rows"] == 4
    # El reporte debe poder volver a pasar por XCom/JSON sin perder nada.
    json.dumps(raw), json.dumps(curated)


def test_duplicates_written_in_two_files_are_caught(lake):
    write(lake / "processed" / "dt=2026-11-04", events(1), name="part-00001.snappy.parquet")
    result = qc.check_raw(str(lake))
    assert result["ok"] is False
    assert result["metrics"]["duplicated_event_ids"] == 1
