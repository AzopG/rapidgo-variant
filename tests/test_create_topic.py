"""Pruebas de la logica de creacion del topic (sin broker)."""

import pytest

from scripts.create_topic import (
    PARTITIONS,
    REPLICATION_FACTOR,
    RETENTION_MS,
    UNASSIGNED_LEADER,
    UNKNOWN_TOPIC,
    TopicNotReady,
    TopicSnapshot,
    describe,
    leaderless_partitions,
    plan,
    wait_for_leaders,
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


# --------------------------------------------------------- liderazgo de particiones
# Un topic recien creado existe en el metadata antes de que Kafka asigne lider a sus
# particiones. Publicar en esa ventana devuelve NotLeaderForPartitionError.


def described(*leaders, error_code=0):
    return [
        {
            "error_code": error_code,
            "partitions": [
                {"partition": index, "leader": leader}
                for index, leader in enumerate(leaders)
            ],
        }
    ]


def test_no_leaderless_partitions_when_every_partition_has_one():
    assert leaderless_partitions(described(1, 2, 3)) == []


def test_leaderless_partitions_reports_those_with_minus_one():
    assert leaderless_partitions(described(1, UNASSIGNED_LEADER, 3)) == [1]


def test_leaderless_partitions_reports_all_when_none_has_leader():
    assert leaderless_partitions(described(*[UNASSIGNED_LEADER] * PARTITIONS)) == [0, 1, 2]


def test_leaderless_partitions_treats_unknown_topic_as_not_ready():
    assert leaderless_partitions(described(error_code=UNKNOWN_TOPIC)) != []


def test_leaderless_partitions_treats_empty_response_as_not_ready():
    assert leaderless_partitions([]) != []


def test_wait_for_leaders_returns_as_soon_as_all_are_assigned():
    responses = [described(-1, -1, -1), described(-1, 2, 3), described(1, 2, 3)]
    sleeps = []

    waited = wait_for_leaders(
        lambda: responses.pop(0),
        timeout=5.0,
        poll_interval=0.5,
        sleep=sleeps.append,
    )

    assert waited == 1.0
    assert sleeps == [0.5, 0.5]


def test_wait_for_leaders_does_not_sleep_when_ready_immediately():
    sleeps = []

    waited = wait_for_leaders(
        lambda: described(1, 2, 3), timeout=5.0, poll_interval=0.5, sleep=sleeps.append
    )

    assert waited == 0.0
    assert sleeps == []


def test_wait_for_leaders_raises_when_the_timeout_expires():
    sleeps = []

    with pytest.raises(TopicNotReady, match="lider"):
        wait_for_leaders(
            lambda: described(-1, -1, -1),
            timeout=1.0,
            poll_interval=0.5,
            sleep=sleeps.append,
        )

    assert sleeps == [0.5, 0.5]


def test_describe_names_the_topic_it_received():
    """Regresion: describe imprimia siempre rapidgo.ratings, aunque el topic fuera otro."""
    text = describe(snapshot(), "rapidgo.ratings.verify.20261029T031633Z")

    assert text.startswith("topic=rapidgo.ratings.verify.20261029T031633Z ")
