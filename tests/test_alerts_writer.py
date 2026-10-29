"""Pruebas de la escritura de alertas: dos queries de streaming, una sola salida.

El defecto que fijan estas pruebas es de concurrencia: A1 y A2 escriben en el mismo
`curated/alerts` desde hilos distintos, y Spark no admite dos committers sobre la
misma salida (`_temporary/0` es unico por ruta). Ver el docstring de
`spark/alerts_writer.py`.
"""

from __future__ import annotations

import threading
import time
from pathlib import Path

from spark.alerts_writer import append_alerts

ALERTS = Path("lake/curated/alerts")
SCHEMA = object()


class Recorder:
    """Estado compartido: cuantas escrituras hay en vuelo a la vez y a donde fueron."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.active = 0
        self.peak = 0
        self.paths: list[str] = []
        self.rows: list[object] = []

    def enter(self, path: str, rows: list[object]) -> None:
        with self._lock:
            self.active += 1
            self.peak = max(self.peak, self.active)
            self.paths.append(path)
            self.rows.extend(rows)

    def leave(self) -> None:
        with self._lock:
            self.active -= 1


class _Writer:
    def __init__(self, recorder: Recorder, records: list[object], delay: float) -> None:
        self.recorder = recorder
        self.records = records
        self.delay = delay

    def mode(self, _mode: str) -> "_Writer":
        return self

    def parquet(self, path: str) -> None:
        self.recorder.enter(path, self.records)
        time.sleep(self.delay)
        self.recorder.leave()


class _Frame:
    def __init__(self, recorder: Recorder, records, delay: float) -> None:
        self.write = _Writer(recorder, list(records), delay)


class FakeSpark:
    """Lo minimo que `append_alerts` usa: createDataFrame + write.mode().parquet()."""

    def __init__(self, recorder: Recorder, delay: float = 0.05) -> None:
        self.recorder = recorder
        self.delay = delay

    def createDataFrame(self, records, _schema):  # noqa: N802 - nombre de Spark
        return _Frame(self.recorder, records, self.delay)


def test_las_dos_queries_nunca_escriben_a_la_vez():
    recorder = Recorder()
    spark = FakeSpark(recorder)
    barrier = threading.Barrier(2)

    def run(alert_id: str) -> None:
        barrier.wait()
        for index in range(5):
            append_alerts(spark, ALERTS, [(alert_id, index)], SCHEMA)

    threads = [
        threading.Thread(target=run, args=(alert_id,)) for alert_id in ("A1", "A2")
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert recorder.peak == 1, "las dos queries escribieron al mismo tiempo"
    assert len(recorder.paths) == 10
    assert recorder.paths == [str(ALERTS)] * 10


def test_sin_filas_no_escribe():
    recorder = Recorder()
    spark = FakeSpark(recorder)

    append_alerts(spark, ALERTS, [], SCHEMA)

    assert recorder.paths == []


def test_escribe_todas_las_filas_en_la_ruta_recibida():
    recorder = Recorder()
    spark = FakeSpark(recorder)
    records = [("A1", 1), ("A2", 2)]

    append_alerts(spark, ALERTS, records, SCHEMA)

    assert recorder.paths == [str(ALERTS)]
    assert recorder.rows == records
