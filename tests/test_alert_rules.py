"""Pruebas de las reglas de alerta A1 y A2 (sin Spark).

Los bordes son los que fija el plan de la semana y los de `docs/kpis-alerts.md:32-35`.
"""

from datetime import datetime, timedelta, timezone

import pytest

from spark.alert_rules import (
    A1_CRITICAL_BELOW,
    A1_MIN_EVENTS,
    A1_WARNING_BELOW,
    A2_CRITICAL_SHARE,
    A2_MIN_COMPLAINTS,
    A2_WARNING_SHARE,
    COOLDOWN,
    GLOBAL_ZONE,
    SEVERITY_CRITICAL,
    SEVERITY_WARNING,
    a1_severity,
    a2_severity,
    cooldown_allows,
    cooldown_key,
)

NOW = datetime(2026, 10, 29, 12, 0, tzinfo=timezone.utc)


def test_thresholds_match_the_documented_values():
    assert (A1_WARNING_BELOW, A1_CRITICAL_BELOW, A1_MIN_EVENTS) == (3.60, 3.20, 20)
    assert (A2_WARNING_SHARE, A2_CRITICAL_SHARE, A2_MIN_COMPLAINTS) == (0.30, 0.50, 10)
    assert COOLDOWN == timedelta(minutes=15)


@pytest.mark.parametrize(
    ("avg_rating", "n", "expected"),
    [
        # los cuatro bordes que fija el plan de la semana
        (3.59, 20, SEVERITY_WARNING),
        (3.19, 20, SEVERITY_CRITICAL),
        (3.60, 20, None),
        (3.59, 19, None),
        # bordes internos
        (3.20, 20, SEVERITY_WARNING),
        (2.00, 1000, SEVERITY_CRITICAL),
        (5.00, 20, None),
        (None, 20, None),
        (2.00, None, None),
    ],
)
def test_a1_borders(avg_rating, n, expected):
    decision = a1_severity(avg_rating, n)
    assert (decision.severity if decision else None) == expected


def test_a1_reports_the_threshold_that_fired():
    assert a1_severity(3.59, 20).threshold == A1_WARNING_BELOW
    assert a1_severity(3.19, 20).threshold == A1_CRITICAL_BELOW


@pytest.mark.parametrize(
    ("share", "n_reclamos", "expected"),
    [
        # los cuatro bordes que fija el plan de la semana
        (0.29, 10, None),
        (0.30, 10, SEVERITY_WARNING),
        (0.49, 10, SEVERITY_WARNING),
        (0.50, 10, SEVERITY_CRITICAL),
        # bordes internos
        (0.30, 9, None),
        (0.50, 9, None),
        (0.90, 40, SEVERITY_CRITICAL),
        (None, 10, None),
        (0.90, None, None),
    ],
)
def test_a2_borders(share, n_reclamos, expected):
    decision = a2_severity(share, n_reclamos)
    assert (decision.severity if decision else None) == expected


def test_a2_share_from_counts_behaves_like_the_documented_share():
    # La participacion real llega como division de conteos, no como literal.
    assert a2_severity(30 / 100, 10).severity == SEVERITY_WARNING
    assert a2_severity(29 / 100, 10) is None


def test_cooldown_allows_first_alert():
    assert cooldown_allows(cooldown_key("A1", "Z-NORTE", None), NOW, {})


def test_cooldown_blocks_within_fifteen_minutes():
    key = cooldown_key("A1", "Z-NORTE", None)
    state = {key: NOW}
    assert not cooldown_allows(key, NOW + timedelta(minutes=14), state)


def test_cooldown_allows_after_fifteen_minutes():
    key = cooldown_key("A1", "Z-NORTE", None)
    state = {key: NOW}
    assert cooldown_allows(key, NOW + timedelta(minutes=15), state)


def test_cooldown_is_per_zone_and_reason():
    state = {cooldown_key("A2", "Z-NORTE", "late_delivery"): NOW}
    assert not cooldown_allows(cooldown_key("A2", "Z-NORTE", "late_delivery"), NOW, state)
    assert cooldown_allows(cooldown_key("A2", "Z-SUR", "late_delivery"), NOW, state)
    assert cooldown_allows(cooldown_key("A2", "Z-NORTE", "cold_food"), NOW, state)
    assert cooldown_allows(cooldown_key("A1", "Z-NORTE", None), NOW, state)


def test_global_zone_key_is_its_own_scope():
    state = {cooldown_key("A1", "Z-NORTE", None): NOW}
    assert cooldown_allows(cooldown_key("A1", GLOBAL_ZONE, None), NOW, state)
