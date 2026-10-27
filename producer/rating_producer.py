"""Generador de eventos `customer_rating` para el topic `rapidgo.ratings`.

Publica eventos que cumplen `docs/event-contract.md` usando `order_id` como key, tal
como define ADR-001: un pedido genera una sola calificacion, asi la deduplicacion
queda dentro de una particion.

El modo de degradacion controlada sube la proporcion de ratings 1-2 y concentra los
motivos en uno solo. Sirve para provocar las alertas A1 y A2 en la corrida integrada
sin esperar a que la produccion se degrade sola.

Uso:
    python -m producer.rating_producer --rate 10 --duration 30 --seed 7
    python -m producer.rating_producer --degradation 0.7 --duration 60

Ejecuta `docker compose up -d kafka` y `python -m scripts.create_topic` antes.
"""

from __future__ import annotations

import argparse
import json
import random
import signal
import sys
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable, Iterator, Mapping

TOPIC = "rapidgo.ratings"
EVENT_TYPE = "customer_rating"
EVENT_VERSION = 1
DEFAULT_BOOTSTRAP = "localhost:9092"

CHANNELS = ("app", "web", "call_center", "chat")
COMPLAINT_REASONS = (
    "late_delivery",
    "cold_food",
    "missing_item",
    "wrong_item",
    "courier_attitude",
    "app_issue",
    "payment_issue",
    "other",
)
ZONES = ("Z-NORTE", "Z-SUR", "Z-CENTRO", "Z-ESTE", "Z-OESTE")

CHANNEL_WEIGHTS = {"app": 0.62, "web": 0.18, "call_center": 0.12, "chat": 0.08}
ZONE_WEIGHTS = {"Z-NORTE": 0.28, "Z-SUR": 0.24, "Z-CENTRO": 0.20, "Z-ESTE": 0.15, "Z-OESTE": 0.13}

# Distribucion normal: promedio esperado ~4.15, por encima del umbral A1 (3.60).
RATING_WEIGHTS_NORMAL = {5: 0.50, 4: 0.28, 3: 0.12, 2: 0.07, 1: 0.03}
# Distribucion degradada: promedio esperado ~2.25, dispara A1 critico.
RATING_WEIGHTS_DEGRADED = {5: 0.05, 4: 0.10, 3: 0.20, 2: 0.35, 1: 0.30}

REASON_WEIGHTS_NORMAL = {
    "late_delivery": 0.38,
    "cold_food": 0.20,
    "missing_item": 0.12,
    "wrong_item": 0.10,
    "courier_attitude": 0.08,
    "app_issue": 0.05,
    "payment_issue": 0.04,
    "other": 0.03,
}


@dataclass(frozen=True)
class ProducerConfig:
    """Parametros de la corrida."""

    rate: float = 5.0
    duration: float = 30.0
    seed: int | None = None
    degradation: float = 0.0
    dominant_reason: str = "late_delivery"
    topic: str = TOPIC
    bootstrap_servers: str = DEFAULT_BOOTSTRAP


@dataclass
class RunReport:
    """Resultado observable de una corrida, para la evidencia del Issue."""

    published: int = 0
    per_partition: dict[int, dict[str, int]] = field(default_factory=dict)
    elapsed_seconds: float = 0.0

    def record(self, partition: int, offset: int) -> None:
        bucket = self.per_partition.setdefault(partition, {"count": 0, "last_offset": -1})
        bucket["count"] += 1
        bucket["last_offset"] = max(bucket["last_offset"], offset)
        self.published += 1

    def summary(self) -> str:
        partitions = ", ".join(
            f"p{p}: count={b['count']} last_offset={b['last_offset']}"
            for p, b in sorted(self.per_partition.items())
        )
        rate = self.published / self.elapsed_seconds if self.elapsed_seconds else 0.0
        return (
            f"publicados={self.published} particiones_usadas={len(self.per_partition)} "
            f"duracion_s={self.elapsed_seconds:.1f} tasa_efectiva={rate:.1f}/s [{partitions}]"
        )


def _weighted_choice(rng: random.Random, weights: Mapping[Any, float]) -> Any:
    keys = list(weights)
    return rng.choices(keys, weights=[weights[k] for k in keys], k=1)[0]


def iso_utc(moment: datetime) -> str:
    """Formatea un datetime en ISO-8601 UTC con milisegundos y sufijo Z."""
    return moment.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def build_event(
    rng: random.Random,
    occurred_at: datetime,
    degradation: float = 0.0,
    dominant_reason: str = "late_delivery",
) -> dict[str, Any]:
    """Construye un evento que cumple el contrato, sin tocar Kafka."""
    degraded = rng.random() < degradation
    rating = _weighted_choice(rng, RATING_WEIGHTS_DEGRADED if degraded else RATING_WEIGHTS_NORMAL)

    complaint_reason = None
    if rating <= 3:
        complaint_reason = (
            dominant_reason
            if degraded
            else _weighted_choice(rng, REASON_WEIGHTS_NORMAL)
        )

    return {
        "event_id": str(uuid.UUID(int=rng.getrandbits(128), version=4)),
        "event_type": EVENT_TYPE,
        "event_version": EVENT_VERSION,
        "occurred_at": iso_utc(occurred_at),
        "order_id": f"ord-{rng.randrange(1, 1_000_000):06d}",
        "customer_id": f"cus-{rng.randrange(1, 5_000):04d}",
        "courier_id": f"cou-{rng.randrange(1, 300):03d}",
        "restaurant_id": f"res-{rng.randrange(1, 400):03d}",
        "zone_id": _weighted_choice(rng, ZONE_WEIGHTS),
        "channel": _weighted_choice(rng, CHANNEL_WEIGHTS),
        "rating": rating,
        "complaint_reason": complaint_reason,
        "comment_length": 0 if rating == 5 and rng.random() < 0.5 else rng.randrange(0, 280),
        "delivery_minutes": round(rng.uniform(15, 70) + (8 if degraded else 0), 1),
        "order_value": round(rng.uniform(8_000, 90_000), 2),
        "ingested_at": None,
    }


def generate_events(config: ProducerConfig, start: datetime | None = None) -> Iterator[dict[str, Any]]:
    """Genera eventos a la tasa configurada hasta agotar la duracion."""
    rng = random.Random(config.seed)
    now = start or datetime.now(timezone.utc)
    interval = 1.0 / config.rate if config.rate > 0 else 0.0
    total = int(config.rate * config.duration)
    for index in range(total):
        yield build_event(
            rng,
            now + timedelta(seconds=index * interval),
            degradation=config.degradation,
            dominant_reason=config.dominant_reason,
        )


def publish(events: Iterable[dict[str, Any]], config: ProducerConfig) -> RunReport:
    """Publica los eventos en Kafka respetando la tasa y devuelve el reporte."""
    from kafka import KafkaProducer

    producer = KafkaProducer(
        bootstrap_servers=config.bootstrap_servers,
        key_serializer=lambda key: key.encode("utf-8"),
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        acks="all",
        linger_ms=5,
        client_id="rapidgo-producer",
    )
    report = RunReport()
    interval = 1.0 / config.rate if config.rate > 0 else 0.0
    started = time.monotonic()
    next_send = started
    try:
        for event in events:
            now = time.monotonic()
            if next_send > now:
                time.sleep(next_send - now)
            next_send += interval
            metadata = producer.send(
                config.topic, key=event["order_id"], value=event
            ).get(timeout=15)
            report.record(metadata.partition, metadata.offset)
        producer.flush(timeout=15)
    finally:
        report.elapsed_seconds = time.monotonic() - started
        producer.close(timeout=15)
    return report


def _stop_on_sigint() -> None:
    def handler(signum, frame):  # noqa: ANN001, ARG001
        raise KeyboardInterrupt

    signal.signal(signal.SIGINT, handler)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Publica eventos customer_rating en Kafka.")
    parser.add_argument("--bootstrap-servers", default=DEFAULT_BOOTSTRAP)
    parser.add_argument("--topic", default=TOPIC)
    parser.add_argument("--rate", type=float, default=5.0, help="eventos por segundo")
    parser.add_argument("--duration", type=float, default=30.0, help="segundos de corrida")
    parser.add_argument("--seed", type=int, default=None, help="semilla para reproducibilidad")
    parser.add_argument(
        "--degradation",
        type=float,
        default=0.0,
        help="proporcion 0..1 de eventos degradados (ratings 1-2 y motivo dominante)",
    )
    parser.add_argument("--dominant-reason", default="late_delivery", choices=COMPLAINT_REASONS)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="genera y muestra eventos sin publicarlos",
    )
    args = parser.parse_args(argv)

    if not 0.0 <= args.degradation <= 1.0:
        print("ERROR: --degradation debe estar entre 0 y 1", file=sys.stderr)
        return 2
    if args.rate <= 0 or args.duration <= 0:
        print("ERROR: --rate y --duration deben ser mayores que 0", file=sys.stderr)
        return 2

    config = ProducerConfig(
        rate=args.rate,
        duration=args.duration,
        seed=args.seed,
        degradation=args.degradation,
        dominant_reason=args.dominant_reason,
        topic=args.topic,
        bootstrap_servers=args.bootstrap_servers,
    )

    if args.dry_run:
        events = list(generate_events(config))
        print(f"generados={len(events)} (dry-run, no se publican)")
        for event in events[:3]:
            print(json.dumps(event, ensure_ascii=False))
        return 0

    _stop_on_sigint()
    print(
        f"publicando en {config.topic} ({config.bootstrap_servers}) "
        f"rate={config.rate}/s duration={config.duration}s "
        f"degradation={config.degradation} seed={config.seed}"
    )
    try:
        report = publish(generate_events(config), config)
    except KeyboardInterrupt:
        print("\ninterrumpido por el usuario", file=sys.stderr)
        return 130
    print(report.summary())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
