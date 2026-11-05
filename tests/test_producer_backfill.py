"""Tests del backfill de eventos, que es lo que le da base a K3 (Issue #15)."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from producer.rating_producer import ProducerConfig, generate_events

NOW = datetime(2026, 11, 4, 12, 0, tzinfo=timezone.utc)


def stamps(events):
    return [
        datetime.fromisoformat(event["occurred_at"].replace("Z", "+00:00"))
        for event in events
    ]


def test_backfill_reparte_los_eventos_en_la_ventana():
    config = ProducerConfig(rate=10, duration=1, seed=1, backfill_minutes=120)
    events = list(generate_events(config, start=NOW))
    moments = stamps(events)
    assert len(events) == 10
    assert moments[0] == NOW - timedelta(minutes=120)
    assert moments[-1] == NOW
    assert moments == sorted(moments)


def test_sin_backfill_los_eventos_empiezan_ahora_y_avanzan():
    config = ProducerConfig(rate=10, duration=1, seed=1)
    moments = stamps(list(generate_events(config, start=NOW)))
    assert moments[0] == NOW
    assert moments[-1] == NOW + timedelta(seconds=0.9)


def test_backfill_no_cambia_la_cantidad_total():
    sin_backfill = list(generate_events(ProducerConfig(rate=5, duration=2, seed=7), start=NOW))
    con_backfill = list(
        generate_events(
            ProducerConfig(rate=5, duration=2, seed=7, backfill_minutes=60), start=NOW
        )
    )
    assert len(sin_backfill) == len(con_backfill) == 10
