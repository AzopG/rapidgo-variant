"""Pruebas de las reglas del stream `customer_rating` (sin Spark).

Cada regla de rechazo de `docs/event-contract.md:43-50` tiene su caso, y los bordes de
las transformaciones quedan fijados aca.
"""

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from spark.transformations import (
    COMPLAINT_REASONS,
    EVENT_TYPE,
    RATING_MAX,
    RATING_MIN,
    is_valid,
    normalize_complaint_reason,
    rating_class,
    validation_reasons,
    zone_region,
)

NOW = datetime(2026, 10, 29, 12, 0, tzinfo=timezone.utc)
SAMPLE = Path(__file__).resolve().parents[1] / "samples" / "rating_event.json"


def sample_event(**overrides):
    event = json.loads(SAMPLE.read_text(encoding="utf-8"))
    event.update(overrides)
    return event


def test_sample_event_is_valid():
    # El sample esta indentado en 18 lineas; como dict (un evento) es valido.
    assert validation_reasons(sample_event(), NOW) == []


def test_missing_required_field_is_rejected():
    event = sample_event()
    del event["customer_id"]
    assert validation_reasons(event, NOW) == ["missing:customer_id"]


def test_rating_out_of_range_is_rejected():
    assert "rating_out_of_range" in validation_reasons(sample_event(rating=0), NOW)
    assert "rating_out_of_range" in validation_reasons(sample_event(rating=6), NOW)


@pytest.mark.parametrize("rating", [RATING_MIN, RATING_MAX])
def test_rating_borders_are_valid(rating):
    event = sample_event(rating=rating, complaint_reason=None if rating >= 4 else "other")
    assert is_valid(event, NOW)


def test_unparsable_occurred_at_is_rejected():
    assert "invalid_occurred_at" in validation_reasons(
        sample_event(occurred_at="22/10/2026 14:32"), NOW
    )


def test_occurred_at_without_timezone_is_rejected():
    assert "invalid_occurred_at" in validation_reasons(
        sample_event(occurred_at="2026-10-29T11:00:00"), NOW
    )


def test_future_occurred_at_beyond_five_minutes_is_rejected():
    future = (NOW + timedelta(minutes=6)).isoformat(timespec="milliseconds")
    assert "future_occurred_at" in validation_reasons(
        sample_event(occurred_at=future), NOW
    )


def test_future_occurred_at_within_five_minutes_is_valid():
    almost = (NOW + timedelta(minutes=4)).isoformat(timespec="milliseconds")
    assert is_valid(sample_event(occurred_at=almost), NOW)


def test_old_event_is_still_valid():
    old = (NOW - timedelta(days=3)).isoformat(timespec="milliseconds")
    assert is_valid(sample_event(occurred_at=old), NOW)


def test_invalid_channel_is_rejected():
    assert "invalid_channel" in validation_reasons(sample_event(channel="telegram"), NOW)


def test_invalid_complaint_reason_is_rejected():
    assert "invalid_complaint_reason" in validation_reasons(
        sample_event(rating=2, complaint_reason="traffic"), NOW
    )


def test_low_rating_without_reason_is_rejected():
    assert "missing_complaint_reason" in validation_reasons(
        sample_event(rating=2, complaint_reason=None), NOW
    )


def test_high_rating_with_reason_is_not_rejected_but_normalized():
    event = sample_event(rating=5, complaint_reason="cold_food")
    assert is_valid(event, NOW)
    assert normalize_complaint_reason(event) is None
    assert normalize_complaint_reason(sample_event(rating=2)) == "late_delivery"


def test_invalid_event_type_is_rejected():
    assert "invalid_event_type" in validation_reasons(
        sample_event(event_type="order_created"), NOW
    )


def test_rejection_reasons_accumulate():
    reasons = validation_reasons(
        sample_event(rating=9, channel="telegram", event_type="order_created"), NOW
    )
    assert {"rating_out_of_range", "invalid_channel", "invalid_event_type"}.issubset(reasons)


@pytest.mark.parametrize(
    ("rating", "expected"),
    [(1, "critico"), (2, "critico"), (3, "medio"), (4, "bueno"), (5, "bueno")],
)
def test_rating_class_borders(rating, expected):
    assert rating_class(rating) == expected


@pytest.mark.parametrize("rating", [None, 0, 6, "4", True])
def test_rating_class_rejects_out_of_contract_values(rating):
    assert rating_class(rating) is None


@pytest.mark.parametrize(
    ("zone_id", "expected"),
    [
        ("Z-NORTE", "norte"),
        ("Z-SUR", "sur"),
        ("Z-CENTRO", "centro"),
        ("Z-ESTE", "este"),
        ("Z-OESTE", "oeste"),
        ("z-norte", None),
        ("Z-PAMPA", None),
        ("NORTE", None),
        (None, None),
    ],
)
def test_zone_region_derivation(zone_id, expected):
    assert zone_region(zone_id) == expected


def test_contract_enums_are_not_empty():
    # Si alguien vacia una enumeracion, los rechazos dejan de tener sentido.
    assert EVENT_TYPE == "customer_rating"
    assert "late_delivery" in COMPLAINT_REASONS
    assert len(COMPLAINT_REASONS) == 8
