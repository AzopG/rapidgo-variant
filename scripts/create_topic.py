"""Crea de forma idempotente el topic `rapidgo.ratings` segun ADR-001.

Reglas que impone el ADR:

- 3 particiones, para que Spark pueda escalar a 3 consumers sin recrear el topic.
- replication-factor 1, porque el entorno local corre un solo broker.
- retention 7 dias (604800000 ms), suficiente para reprocesar una semana.
- auto-create deshabilitado en el broker: el topic existe porque este script lo crea.

El script no borra ni recrea nada: si el topic ya existe con la configuracion
correcta, no hace nada. Si existe con particiones o replication-factor distintos,
falla con un mensaje explicito porque esas dos propiedades no se pueden reducir sin
perder datos.

Uso:
    python -m scripts.create_topic
    python -m scripts.create_topic --bootstrap-servers localhost:9092 --dry-run
"""

from __future__ import annotations

import argparse
import sys
import time
from dataclasses import dataclass
from typing import Any, Callable, Mapping

from kafka.admin import (
    ConfigResource,
    ConfigResourceType,
    KafkaAdminClient,
    NewTopic,
)

TOPIC = "rapidgo.ratings"
PARTITIONS = 3
REPLICATION_FACTOR = 1
RETENTION_MS = 7 * 24 * 60 * 60 * 1000
CLEANUP_POLICY = "delete"
DEFAULT_BOOTSTRAP = "localhost:9092"

# Codigo de error que devuelve Kafka cuando el topic no existe.
UNKNOWN_TOPIC = 3

# Lider que Kafka usa para "todavia no asignado".
UNASSIGNED_LEADER = -1

# Kafka publica el topic en el metadata antes de elegir lider de sus particiones.
# Escribir en esa ventana devuelve NotLeaderForPartitionError, asi que el script espera.
LEADER_TIMEOUT_SECONDS = 30.0
LEADER_POLL_SECONDS = 0.2


class TopicNotReady(RuntimeError):
    """El topic existe pero Kafka no asigno lider a todas sus particiones a tiempo."""


@dataclass(frozen=True)
class TopicSnapshot:
    """Estado observado del topic en el broker."""

    exists: bool
    partitions: int = 0
    replication_factor: int = 0
    retention_ms: int | None = None
    cleanup_policy: str | None = None


def plan(snapshot: TopicSnapshot) -> str:
    """Decide la accion necesaria para alinear el topic con ADR-001.

    Devuelve uno de: ``create``, ``noop``, ``update-config``,
    ``reject-partitions`` o ``reject-replication``.
    """
    if not snapshot.exists:
        return "create"
    if snapshot.partitions != PARTITIONS:
        return "reject-partitions"
    if snapshot.replication_factor != REPLICATION_FACTOR:
        return "reject-replication"
    if snapshot.retention_ms != RETENTION_MS:
        return "update-config"
    if snapshot.cleanup_policy not in (None, CLEANUP_POLICY):
        return "update-config"
    return "noop"


def _as_int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def read_snapshot(admin: KafkaAdminClient, topic: str = TOPIC) -> TopicSnapshot:
    """Lee particiones, replication-factor y retencion del topic."""
    described = admin.describe_topics([topic])
    if not described or described[0].get("error_code") == UNKNOWN_TOPIC:
        return TopicSnapshot(exists=False)

    partitions = described[0].get("partitions") or []
    if not partitions:
        return TopicSnapshot(exists=False)

    config = _config_map(admin, topic)
    return TopicSnapshot(
        exists=True,
        partitions=len(partitions),
        replication_factor=len(partitions[0].get("replicas") or []),
        retention_ms=_as_int(config.get("retention.ms")),
        cleanup_policy=config.get("cleanup.policy"),
    )


def _config_map(admin: KafkaAdminClient, topic: str) -> Mapping[str, str]:
    """Devuelve las configuraciones del topic como dict nombre -> valor.

    kafka-python-ng expone la respuesta como una lista de namedtuples con un campo
    ``resources``; cada recurso es una tupla plana y cada configuracion es
    ``(nombre, valor, read_only, ...)``. Se soportan tambien objetos por atributo
    para no quedar atados a esa representacion.
    """
    response = admin.describe_configs([ConfigResource(ConfigResourceType.TOPIC, topic)])
    entries: dict[str, str] = {}
    for resource in _iter_resources(response):
        for entry in _iter_config_entries(resource):
            name = _entry_field(entry, "name", 0)
            value = _entry_field(entry, "value", 1)
            if name:
                entries[name] = value
    return entries


def _iter_resources(response: Any) -> list[Any]:
    resources: list[Any] = []
    for item in response or []:
        nested = getattr(item, "resources", None)
        if nested is not None:
            resources.extend(nested)
        else:
            resources.append(item)
    return resources


def _iter_config_entries(resource: Any) -> list[Any]:
    entries = getattr(resource, "config_entries", None)
    if entries is not None:
        return list(entries)
    if isinstance(resource, tuple) and len(resource) > 4:
        return list(resource[4] or [])
    return []


def _entry_field(entry: Any, attribute: str, position: int) -> Any:
    value = getattr(entry, attribute, None)
    if value is not None:
        return value
    if isinstance(entry, tuple) and len(entry) > position:
        return entry[position]
    return None


def apply(
    admin: KafkaAdminClient,
    action: str,
    snapshot: TopicSnapshot,
    topic: str = TOPIC,
    dry_run: bool = False,
) -> None:
    """Ejecuta la accion decidida por :func:`plan`."""
    if action in ("reject-partitions", "reject-replication"):
        raise SystemExit(
            f"ERROR: {topic} existe con {snapshot.partitions} particiones y "
            f"replication-factor {snapshot.replication_factor}; ADR-001 exige "
            f"{PARTITIONS} y {REPLICATION_FACTOR}. Esas propiedades no se pueden "
            f"reducir sin perder datos: revisar el broker antes de continuar."
        )

    if action == "noop":
        print(f"{topic}: ya cumple ADR-001, no se modifica")
        return

    if dry_run:
        print(f"{topic}: accion pendiente = {action} (dry-run, no se aplica)")
        return

    if action == "create":
        admin.create_topics(
            [
                NewTopic(
                    name=topic,
                    num_partitions=PARTITIONS,
                    replication_factor=REPLICATION_FACTOR,
                    topic_configs={
                        "retention.ms": str(RETENTION_MS),
                        "cleanup.policy": CLEANUP_POLICY,
                    },
                )
            ],
            validate_only=False,
        )
        print(f"{topic}: creado")
        return

    if action == "update-config":
        admin.alter_configs(
            [
                ConfigResource(
                    ConfigResourceType.TOPIC,
                    topic,
                    configs={
                        "retention.ms": str(RETENTION_MS),
                        "cleanup.policy": CLEANUP_POLICY,
                    },
                )
            ]
        )
        print(f"{topic}: configuracion actualizada")


def leaderless_partitions(described: Any) -> list[int]:
    """IDs de las particiones que todavia no tienen lider asignado.

    Una lista vacia significa que el topic ya acepta escrituras. Cuando la respuesta
    no alcanza para saberlo (el topic aun no es visible en el metadata) se devuelve el
    centinela, para que el llamador siga esperando en vez de publicar sobre el vacio.
    """
    first = (described or [None])[0]
    if not first:
        return [UNASSIGNED_LEADER]
    if first.get("error_code") not in (None, 0):
        return [UNASSIGNED_LEADER]
    partitions = first.get("partitions") or []
    if not partitions:
        return [UNASSIGNED_LEADER]
    return [
        partition.get("partition", UNASSIGNED_LEADER)
        for partition in partitions
        if partition.get("leader") in (None, UNASSIGNED_LEADER)
    ]


def wait_for_leaders(
    describe_topics: Callable[[], Any],
    timeout: float = LEADER_TIMEOUT_SECONDS,
    poll_interval: float = LEADER_POLL_SECONDS,
    sleep: Callable[[float], None] = time.sleep,
) -> float:
    """Espera a que todas las particiones tengan lider y devuelve los segundos esperados.

    Ataca la causa y no el sintoma: un producer con retries sobrevive a
    ``NotLeaderForPartitionError``, pero la corrida se vuelve lenta y la evidencia
    queda llena de reintentos. Esperar aca es deterministico y se mide.
    """
    waited = 0.0
    while True:
        pending = leaderless_partitions(describe_topics())
        if not pending:
            return waited
        if waited >= timeout:
            raise TopicNotReady(
                f"las particiones {pending} siguen sin lider despues de {timeout:g}s"
            )
        sleep(poll_interval)
        waited += poll_interval


def describe(snapshot: TopicSnapshot, topic: str = TOPIC) -> str:
    """Resumen legible del topic para la evidencia del Issue."""
    retention_days = None
    if snapshot.retention_ms is not None:
        retention_days = snapshot.retention_ms / 86_400_000
    return (
        f"topic={topic} exists={snapshot.exists} "
        f"partitions={snapshot.partitions} "
        f"replication_factor={snapshot.replication_factor} "
        f"retention_ms={snapshot.retention_ms} "
        f"retention_days={retention_days} "
        f"cleanup_policy={snapshot.cleanup_policy}"
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Crea el topic rapidgo.ratings segun ADR-001."
    )
    parser.add_argument("--bootstrap-servers", default=DEFAULT_BOOTSTRAP)
    parser.add_argument("--topic", default=TOPIC)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Muestra la accion que aplicaria sin modificar el broker.",
    )
    parser.add_argument(
        "--leader-timeout",
        type=float,
        default=LEADER_TIMEOUT_SECONDS,
        help="Segundos maximos a esperar por el lider de las particiones.",
    )
    args = parser.parse_args(argv)

    admin = KafkaAdminClient(
        bootstrap_servers=args.bootstrap_servers,
        client_id="rapidgo-create-topic",
        request_timeout_ms=15_000,
    )
    try:
        snapshot = read_snapshot(admin, args.topic)
        action = plan(snapshot)
        print(f"estado observado: {describe(snapshot, args.topic)}")
        print(f"accion: {action}")
        apply(admin, action, snapshot, args.topic, args.dry_run)

        if args.dry_run:
            return 0

        try:
            waited = wait_for_leaders(
                lambda: admin.describe_topics([args.topic]),
                timeout=args.leader_timeout,
            )
        except TopicNotReady as exc:
            print(f"ERROR: {exc}", file=sys.stderr)
            return 1
        if waited:
            print(f"{args.topic}: lideres asignados tras {waited:.1f}s")

        final = read_snapshot(admin, args.topic)
        print(f"estado final: {describe(final, args.topic)}")
        if plan(final) != "noop":
            print("ERROR: el topic no quedo en el estado esperado", file=sys.stderr)
            return 1
        return 0
    finally:
        admin.close()


if __name__ == "__main__":
    raise SystemExit(main())
