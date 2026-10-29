"""Pruebas del producer de calificaciones (sin Kafka)."""

import random
from datetime import datetime, timedelta, timezone

import pytest

from producer.rating_producer import (
    CHANNELS,
    COMPLAINT_REASONS,
    EVENT_TYPE,
    EVENT_VERSION,
    PRODUCER_RETRIES,
    PRODUCER_RETRY_BACKOFF_MS,
    ProducerConfig,
    RunReport,
    build_event,
    generate_events,
    iso_utc,
    publish,
)

NOW = datetime(2026, 10, 29, 12, 0, tzinfo=timezone.utc)


def make_events(count=200, seed=7, degradation=0.0):
    rng = random.Random(seed)
    return [
        build_event(rng, NOW + timedelta(seconds=i), degradation=degradation)
        for i in range(count)
    ]


def test_event_has_contract_fields():
    event = make_events(1)[0]
    required = {
        "event_id",
        "event_type",
        "event_version",
        "occurred_at",
        "order_id",
        "customer_id",
        "zone_id",
        "channel",
        "rating",
        "complaint_reason",
        "ingested_at",
    }
    assert required.issubset(event)


def test_event_type_and_version_are_fixed():
    event = make_events(1)[0]
    assert event["event_type"] == EVENT_TYPE == "customer_rating"
    assert event["event_version"] == EVENT_VERSION == 1


def test_rating_inside_contract_range():
    for event in make_events(300):
        assert 1 <= event["rating"] <= 5


def test_channel_and_reason_belong_to_enums():
    for event in make_events(300):
        assert event["channel"] in CHANNELS
        if event["complaint_reason"] is not None:
            assert event["complaint_reason"] in COMPLAINT_REASONS


def test_low_rating_requires_complaint_reason():
    for event in make_events(400):
        if event["rating"] <= 3:
            assert event["complaint_reason"] is not None


def test_high_rating_has_no_complaint_reason():
    for event in make_events(400):
        if event["rating"] >= 4:
            assert event["complaint_reason"] is None


def test_ingested_at_is_null_from_producer():
    assert all(event["ingested_at"] is None for event in make_events(50))


def test_occurred_at_is_utc_iso_with_z():
    event = make_events(1)[0]
    assert event["occurred_at"].endswith("Z")
    parsed = datetime.fromisoformat(event["occurred_at"].replace("Z", "+00:00"))
    assert parsed.tzinfo is not None
    assert parsed.utcoffset() == timedelta(0)


def test_iso_utc_formats_milliseconds():
    moment = datetime(2026, 10, 29, 12, 0, 0, 512000, tzinfo=timezone.utc)
    assert iso_utc(moment) == "2026-10-29T12:00:00.512Z"


def test_event_ids_are_unique():
    ids = [event["event_id"] for event in make_events(500)]
    assert len(ids) == len(set(ids))


def test_same_seed_produces_identical_events():
    assert make_events(20, seed=42) == make_events(20, seed=42)


def test_different_seed_produces_different_events():
    assert make_events(20, seed=1) != make_events(20, seed=2)


def test_degradation_increases_low_ratings_and_concentrates_reason():
    normal = make_events(600, seed=11, degradation=0.0)
    degraded = make_events(600, seed=11, degradation=0.8)

    low_normal = sum(1 for e in normal if e["rating"] <= 2)
    low_degraded = sum(1 for e in degraded if e["rating"] <= 2)
    assert low_degraded > low_normal * 3

    reasons = [e["complaint_reason"] for e in degraded if e["complaint_reason"]]
    dominant = sum(1 for r in reasons if r == "late_delivery")
    assert dominant / len(reasons) > 0.5


def test_generate_events_respects_rate_and_duration():
    config = ProducerConfig(rate=10, duration=2.5, seed=3)
    events = list(generate_events(config, start=NOW))
    assert len(events) == 25
    assert events[0]["occurred_at"] == "2026-10-29T12:00:00.000Z"
    assert events[-1]["occurred_at"] == "2026-10-29T12:00:02.400Z"


def test_run_report_accumulates_offsets_per_partition():
    report = RunReport()
    report.record(0, 10)
    report.record(0, 11)
    report.record(2, 4)
    report.elapsed_seconds = 2.0
    assert report.published == 3
    assert report.per_partition[0] == {"count": 2, "last_offset": 11}
    assert report.per_partition[2] == {"count": 1, "last_offset": 4}
    assert "publicados=3" in report.summary()
    assert "ultimo" not in report.summary()


@pytest.mark.parametrize("degradation", [-0.1, 1.1])
def test_build_event_accepts_any_degradation_range(degradation):
    # El rango se valida en la CLI; la funcion pura no debe romper.
    event = build_event(random.Random(1), NOW, degradation=degradation)
    assert 1 <= event["rating"] <= 5


# ------------------------------------------------------------------ publicacion real
# NotLeaderForPartition es un error retriable de Kafka: sin retries el primer send
# contra un topic recien creado mata la corrida entera.


class _Metadata:
    partition = 0
    offset = 0


class _Future:
    def get(self, timeout=None):  # noqa: ARG002
        return _Metadata()


def _capture_producer_kwargs(monkeypatch):
    captured = {}

    class FakeProducer:
        def __init__(self, **kwargs):
            captured.update(kwargs)

        def send(self, *args, **kwargs):  # noqa: ARG002
            return _Future()

        def flush(self, timeout=None):  # noqa: ARG002
            return None

        def close(self, timeout=None):  # noqa: ARG002
            return None

    monkeypatch.setattr("kafka.KafkaProducer", FakeProducer)
    return captured


def test_publish_retries_transient_kafka_errors(monkeypatch):
    captured = _capture_producer_kwargs(monkeypatch)

    publish([build_event(random.Random(1), NOW)], ProducerConfig(rate=1.0, duration=1.0))

    assert captured["retries"] == PRODUCER_RETRIES >= 1
    assert captured["retry_backoff_ms"] == PRODUCER_RETRY_BACKOFF_MS >= 100


def test_publish_keeps_acks_all_and_publishes(monkeypatch):
    captured = _capture_producer_kwargs(monkeypatch)
    event = build_event(random.Random(1), NOW)

    report = publish([event], ProducerConfig(rate=1.0, duration=1.0))

    assert captured["acks"] == "all"
    assert report.published == 1