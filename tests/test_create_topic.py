"""Pruebas de la logica de creacion del topic (sin broker)."""

from scripts.create_topic import (
    PARTITIONS,
    REPLICATION_FACTOR,
    RETENTION_MS,
    TopicSnapshot,
    plan,
)


def snapshot(**overrides):
    base = dict(
        exists=True,
        partitions=PARTITIONS,
        replication_factor=REPLICATION_FACTOR,
        retention_ms=RETENTION_MS,
        cleanup_policy="delete",
    )
    base.update(overrides)
    return TopicSnapshot(**base)


def test_adr001_constants():
    assert PARTITIONS == 3
    assert REPLICATION_FACTOR == 1
    assert RETENTION_MS == 604_800_000
    assert RETENTION_MS == 7 * 24 * 60 * 60 * 1000


def test_creates_when_topic_missing():
    assert plan(TopicSnapshot(exists=False)) == "create"


def test_noop_when_topic_matches_adr():
    assert plan(snapshot()) == "noop"


def test_noop_when_cleanup_policy_not_reported():
    assert plan(snapshot(cleanup_policy=None)) == "noop"


def test_updates_when_retention_differs():
    assert plan(snapshot(retention_ms=86_400_000)) == "update-config"


def test_updates_when_cleanup_policy_is_compact():
    assert plan(snapshot(cleanup_policy="compact")) == "update-config"


def test_rejects_partition_mismatch():
    assert plan(snapshot(partitions=1)) == "reject-partitions"


def test_rejects_replication_mismatch():
    assert plan(snapshot(replication_factor=2)) == "reject-replication"
