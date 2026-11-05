"""Regresiones de la fase 0: los defectos heredados de la semana 2.

Cada prueba fija uno de los ocho arreglos de `docs/plan-semana3.md` §5. Las que
construyen el plan de Spark usan un doble de `pyspark.sql.functions`: no levantan una
sesion ni un contenedor, y por eso corren en el mismo segundo que el resto de la suite.
"""

from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from spark import alert_rules, alerts_job, streaming_job, transformations
from spark.transformations import OCCURRED_AT_TZ_PATTERN, REASON_SEPARATOR, parse_occurred_at

NOW = datetime(2026, 10, 29, 12, 0, tzinfo=timezone.utc)
LAKE = Path("lake")


# --------------------------------------------------------------------- dobles


class FakeNode:
    """Expresion de Spark falsa: registra cada operacion y encadena sin JVM."""

    def __init__(self, op, args=(), log=None):
        self.op = op
        self.args = args
        self.log = log

    def __getattr__(self, name):
        def method(*args, **kwargs):
            if self.log is not None:
                self.log.append((name, args))
            return FakeNode(name, (self, *args), self.log)

        return method

    def __and__(self, other):
        return FakeNode("and", (self, other), self.log)

    def __or__(self, other):
        return FakeNode("or", (self, other), self.log)

    def __invert__(self):
        return FakeNode("not", (self,), self.log)

    def __gt__(self, other):
        return FakeNode("gt", (self, other), self.log)

    def __ge__(self, other):
        return FakeNode("ge", (self, other), self.log)

    def __lt__(self, other):
        return FakeNode("lt", (self, other), self.log)

    def __le__(self, other):
        return FakeNode("le", (self, other), self.log)

    def __eq__(self, other):
        return FakeNode("eq", (self, other), self.log)

    def __ne__(self, other):
        return FakeNode("ne", (self, other), self.log)

    def __add__(self, other):
        return FakeNode("add", (self, other), self.log)

    def __hash__(self):
        return id(self)


class FakeFunctions:
    """Lo minimo de `pyspark.sql.functions` que arman los planes bajo prueba."""

    def __init__(self):
        self.calls: list[tuple[str, tuple]] = []

    def _node(self, op, *args):
        return FakeNode(op, args, self.calls)

    def col(self, name):
        return self._node("col", name)

    def lit(self, value):
        return self._node("lit", value)

    def expr(self, value):
        return self._node("expr", value)

    def when(self, condition, value):
        return self._node("when", condition, value)

    def nullif(self, first, second):
        return self._node("nullif", first, second)

    def concat_ws(self, separator, *columns):
        self.calls.append(("concat_ws", (separator,)))
        return self._node("concat_ws", separator, *columns)

    def coalesce(self, *columns):
        self.calls.append(("coalesce", columns))
        return self._node("coalesce", *columns)

    def date_format(self, column, fmt):
        return self._node("date_format", column, fmt)

    def window(self, column, duration, slide=None):
        self.calls.append(("window", (duration, slide)))
        return self._node("window", duration, slide)

    def avg(self, column):
        return self._node("avg", column)

    def count(self, column):
        return self._node("count", column)

    def sum(self, column):
        return self._node("sum", column)


class FakeFrame:
    """DataFrame falso: cada transformacion queda registrada y se devuelve a si mismo."""

    def __init__(self):
        self.calls: list[tuple] = []

    def unionByName(self, other):  # noqa: N802 - nombre de Spark
        self.calls.append(("unionByName",))
        return self

    def withColumn(self, name, column):  # noqa: N802 - nombre de Spark
        self.calls.append(("withColumn", name))
        return self

    def groupBy(self, *columns):  # noqa: N802 - nombre de Spark
        self.calls.append(("groupBy", columns))
        return self

    def agg(self, *columns):
        self.calls.append(("agg", columns))
        return self

    def where(self, condition):
        self.calls.append(("where", condition))
        return self

    def withWatermark(self, column, delay):  # noqa: N802 - nombre de Spark
        self.calls.append(("withWatermark", column, delay))
        return self

    def dropDuplicates(self, columns):  # noqa: N802 - nombre de Spark
        self.calls.append(("dropDuplicates", list(columns)))
        return self

    def select(self, *columns):
        self.calls.append(("select", columns))
        return self


@pytest.fixture
def fake_functions(monkeypatch):
    """Instala un `_functions()` falso en el modulo indicado."""

    def install(module):
        fake = FakeFunctions()
        monkeypatch.setattr(module, "_functions", lambda: fake)
        return fake

    return install


class FakeRow:
    def __init__(self, **values):
        self.__dict__.update(values)

    def __getitem__(self, name):
        return self.__dict__[name]


class FakeBatch:
    def __init__(self, rows):
        self._rows = rows
        self.sparkSession = None

    def collect(self):
        return self._rows


def a1_row(avg_rating, n, zone_id, moment):
    return FakeRow(
        avg_rating=avg_rating,
        n=n,
        zone_id=zone_id,
        window={"start": moment - timedelta(minutes=10), "end": moment},
    )


def a2_row(total, counts, zone_id, moment):
    row = FakeRow(
        total_reclamos=total,
        zone_id=zone_id,
        window={"start": moment - timedelta(minutes=15), "end": moment},
    )
    for index, count in enumerate(counts):
        setattr(row, f"n_motivo_{index}", count)
    return row


# ------------------------------------------------- 1. cooldown despues de escribir


def test_a1_no_marca_el_cooldown_si_la_escritura_falla(monkeypatch):
    state: dict[tuple, datetime] = {}
    batch = FakeBatch([a1_row(3.0, 20, "Z-NORTE", NOW)])
    attempts: list[list[tuple]] = []

    def failing_emit(spark, lake, records):
        attempts.append(records)
        raise RuntimeError("lake caido")

    monkeypatch.setattr(alerts_job, "_emit", failing_emit)
    with pytest.raises(RuntimeError):
        alerts_job.write_a1(batch, 0, LAKE, state)

    assert state == {}, "el cooldown no puede marcarse si la alerta no se escribio"

    monkeypatch.setattr(
        alerts_job, "_emit", lambda spark, lake, records: attempts.append(records)
    )
    alerts_job.write_a1(batch, 1, LAKE, state)

    assert len(attempts) == 2
    assert attempts[1], "el reintento tiene que volver a emitir la alerta"
    assert state, "recien con la escritura exitosa queda marcado el cooldown"


def test_a2_no_marca_el_cooldown_si_la_escritura_falla(monkeypatch):
    state: dict[tuple, datetime] = {}
    batch = FakeBatch([a2_row(20, [10, 10, 0, 0, 0, 0, 0, 0], "Z-NORTE", NOW)])
    attempts: list[list[tuple]] = []

    def failing_emit(spark, lake, records):
        attempts.append(records)
        raise RuntimeError("lake caido")

    monkeypatch.setattr(alerts_job, "_emit", failing_emit)
    with pytest.raises(RuntimeError):
        alerts_job.write_a2(batch, 0, LAKE, state)

    assert state == {}
    assert attempts and attempts[0]


# --------------------------------------------------- 2. ventanas deslizantes cada minuto


def test_a1_y_a2_usan_ventanas_deslizantes(fake_functions):
    fake = fake_functions(alerts_job)
    alerts_job.a1_stream(FakeFrame())
    alerts_job.a2_stream(FakeFrame())

    windows = [args for name, args in fake.calls if name == "window"]
    assert windows == [
        (alert_rules.A1_WINDOW, alert_rules.A1_SLIDE),
        (alert_rules.A2_WINDOW, alert_rules.A2_SLIDE),
    ]
    assert alert_rules.A1_SLIDE == alert_rules.A2_SLIDE == "1 minute"


# --------------------------------------------- 3. deduplicacion del stream de rechazados


def test_los_rechazados_se_deduplican_igual_que_los_validos(fake_functions):
    fake = fake_functions(streaming_job)
    frame = FakeFrame()

    streaming_job.rejected_stream(frame)

    assert ("withWatermark", "kafka_ts", streaming_job.WATERMARK) in frame.calls
    assert ("dropDuplicates", [streaming_job.REJECTED_DEDUP_COLUMN]) in frame.calls

    coalesce = [args for name, args in fake.calls if name == "coalesce"]
    assert coalesce, "la identidad del rechazado combina event_id y posicion en Kafka"
    first = coalesce[0][0]
    assert first.op == "col"
    assert first.args == ("event_id",)


# ------------------------------------------------ 4. un solo separador en los motivos


def test_los_motivos_compuestos_usan_un_solo_separador(fake_functions):
    fake = fake_functions(transformations)

    transformations.rejection_reason_column(NOW)

    separators = [args[0] for name, args in fake.calls if name == "concat_ws"]
    assert separators == [REASON_SEPARATOR, REASON_SEPARATOR]
    assert REASON_SEPARATOR == ";"


# ------------------------------------------------------ 5. sin clasificacion muerta


def test_no_queda_la_clasificacion_muerta():
    assert not hasattr(transformations, "RATING_CLASSES")


# --------------------------------------------------- 6. el contador del log no miente


def test_a1_cuenta_ventanas_evaluadas_y_no_alertas(monkeypatch, capsys):
    monkeypatch.setattr(alerts_job, "_emit", lambda *args: None)
    rows = [
        a1_row(4.9, 20, "Z-NORTE", NOW),  # evaluada, no dispara
        a1_row(3.0, 20, "Z-SUR", NOW),  # evaluada, dispara
    ]

    alerts_job.write_a1(FakeBatch(rows), 0, LAKE, {})

    output = capsys.readouterr().out
    assert "evaluadas=2" in output
    assert "alertas=1" in output


def test_a2_cuenta_ventanas_evaluadas_y_no_motivos_disparados(monkeypatch, capsys):
    monkeypatch.setattr(alerts_job, "_emit", lambda *args: None)
    # Una ventana con dos motivos que disparan: evaluadas=1, alertas=2.
    row = a2_row(20, [10, 10, 0, 0, 0, 0, 0, 0], "Z-NORTE", NOW)

    alerts_job.write_a2(FakeBatch([row]), 0, LAKE, {})

    output = capsys.readouterr().out
    assert "evaluadas=1" in output
    assert "alertas=2" in output


# ------------------------------------------- 7. zona horaria explicita, en job y validador

EXPLICIT_TIMEZONES = [
    "2026-10-29T11:00:00Z",
    "2026-10-29T11:00:00.512Z",
    "2026-10-29T11:00:00+00:00",
    "2026-10-29T11:00:00-03:00",
]
MISSING_TIMEZONES = ["2026-10-29T11:00:00", "2026-10-29 11:00:00"]


@pytest.mark.parametrize("value", EXPLICIT_TIMEZONES)
def test_la_regla_de_zona_acepta_un_offset_explicito(value):
    assert re.search(OCCURRED_AT_TZ_PATTERN, value)
    assert parse_occurred_at(value) is not None
    assert transformations.occurred_at_has_explicit_timezone(value)


@pytest.mark.parametrize("value", MISSING_TIMEZONES)
def test_la_regla_de_zona_rechaza_un_timestamp_sin_zona(value):
    assert not re.search(OCCURRED_AT_TZ_PATTERN, value)
    assert parse_occurred_at(value) is None
    assert not transformations.occurred_at_has_explicit_timezone(value)


def test_el_job_aplica_la_misma_regla_de_zona_que_el_validador(fake_functions):
    fake = fake_functions(transformations)

    transformations.rejection_reason_column(NOW)

    assert ("rlike", (OCCURRED_AT_TZ_PATTERN,)) in fake.calls
