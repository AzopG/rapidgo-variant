"""Reglas del stream `customer_rating`: esquema, validacion y transformaciones.

Este modulo es la unica fuente de verdad de las reglas de `docs/event-contract.md`.
Las funciones puras (`validation_reasons`, `rating_class`, `zone_region`) se prueban
sin Spark; el job arma sus expresiones a partir de las mismas constantes, asi que un
cambio de umbral o de enum se hace en un solo lugar.

Las expresiones Spark viven en :func:`rejection_reason_column` y
:func:`transform_columns`; las importa `spark/streaming_job.py`.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Mapping

EVENT_TYPE = "customer_rating"
EVENT_VERSION = 1
TOPIC = "rapidgo.ratings"

CHANNELS: tuple[str, ...] = ("app", "web", "call_center", "chat")
COMPLAINT_REASONS: tuple[str, ...] = (
    "late_delivery",
    "cold_food",
    "missing_item",
    "wrong_item",
    "courier_attitude",
    "app_issue",
    "payment_issue",
    "other",
)
ZONE_REGIONS: tuple[str, ...] = ("norte", "sur", "centro", "este", "oeste")

RATING_MIN = 1
RATING_MAX = 5
# rating <= LOW_RATING_MAX obliga a traer motivo; rating >= HIGH_RATING_MIN no lo admite.
LOW_RATING_MAX = 3
HIGH_RATING_MIN = 4

# Un evento con occurred_at mas de 5 minutos en el futuro se descarta.
MAX_FUTURE_SKEW = timedelta(minutes=5)

# Clasificacion del rating (transformacion 2 del Issue #8).
RATING_CLASSES: dict[str, str] = {
    "critico": "rating_1_2",
    "medio": "rating_3",
    "bueno": "rating_4_5",
}

WATERMARK = "30 minutes"

EVENT_SCHEMA_SQL = """
    event_id STRING,
    event_type STRING,
    event_version INT,
    occurred_at STRING,
    order_id STRING,
    customer_id STRING,
    courier_id STRING,
    restaurant_id STRING,
    zone_id STRING,
    channel STRING,
    rating INT,
    complaint_reason STRING,
    comment_length INT,
    delivery_minutes DOUBLE,
    order_value DOUBLE,
    ingested_at STRING
"""

REQUIRED_FIELDS: tuple[str, ...] = (
    "event_id",
    "event_type",
    "event_version",
    "occurred_at",
    "order_id",
    "customer_id",
    "zone_id",
    "channel",
    "rating",
)

# Motivos de rechazo. Son estables: `datalake/curated/ratings_rejected` los guarda tal cual.
REASON_MISSING_PREFIX = "missing"
REASON_INVALID_EVENT_TYPE = "invalid_event_type"
REASON_RATING_OUT_OF_RANGE = "rating_out_of_range"
REASON_INVALID_OCCURRED_AT = "invalid_occurred_at"
REASON_FUTURE_OCCURRED_AT = "future_occurred_at"
REASON_INVALID_CHANNEL = "invalid_channel"
REASON_INVALID_COMPLAINT_REASON = "invalid_complaint_reason"
REASON_MISSING_COMPLAINT_REASON = "missing_complaint_reason"


def rating_class(rating: Any) -> str | None:
    """Clasifica el rating en critico (1-2), medio (3) o bueno (4-5)."""
    if not isinstance(rating, int) or isinstance(rating, bool):
        return None
    if rating < RATING_MIN or rating > RATING_MAX:
        return None
    if rating <= 2:
        return "critico"
    if rating == LOW_RATING_MAX:
        return "medio"
    return "bueno"


def zone_region(zone_id: Any) -> str | None:
    """Deriva la region de `Z-NORTE`. Devuelve None si el formato no es el esperado."""
    if not isinstance(zone_id, str) or not zone_id.startswith("Z-"):
        return None
    region = zone_id[2:].strip().lower()
    return region if region in ZONE_REGIONS else None


def parse_occurred_at(value: Any) -> datetime | None:
    """Parsea `occurred_at` ISO-8601. Exige zona horaria: el contrato dice UTC."""
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return None
    return parsed.astimezone(timezone.utc)


def normalize_complaint_reason(event: Mapping[str, Any]) -> str | None:
    """Regla 6 del contrato: con rating >= 4 el motivo se normaliza a null."""
    rating = event.get("rating")
    if isinstance(rating, int) and rating >= HIGH_RATING_MIN:
        return None
    return event.get("complaint_reason")


def validation_reasons(event: Mapping[str, Any], now: datetime | None = None) -> list[str]:
    """Devuelve los motivos de rechazo del evento. Lista vacia = evento valido.

    Sigue el orden de `docs/event-contract.md:43-50`. La regla 6 no rechaza: se
    resuelve en :func:`normalize_complaint_reason`.
    """
    reasons: list[str] = []
    now = now or datetime.now(timezone.utc)

    for field in REQUIRED_FIELDS:
        if event.get(field) is None:
            reasons.append(f"{REASON_MISSING_PREFIX}:{field}")

    if event.get("event_type") is not None and event.get("event_type") != EVENT_TYPE:
        reasons.append(REASON_INVALID_EVENT_TYPE)

    rating = event.get("rating")
    if rating is not None and (not isinstance(rating, int) or isinstance(rating, bool)
                               or not RATING_MIN <= rating <= RATING_MAX):
        reasons.append(REASON_RATING_OUT_OF_RANGE)

    if event.get("occurred_at") is not None:
        occurred_at = parse_occurred_at(event.get("occurred_at"))
        if occurred_at is None:
            reasons.append(REASON_INVALID_OCCURRED_AT)
        elif occurred_at - now > MAX_FUTURE_SKEW:
            reasons.append(REASON_FUTURE_OCCURRED_AT)

    channel = event.get("channel")
    if channel is not None and channel not in CHANNELS:
        reasons.append(REASON_INVALID_CHANNEL)

    reason = event.get("complaint_reason")
    if reason is not None and reason not in COMPLAINT_REASONS:
        reasons.append(REASON_INVALID_COMPLAINT_REASON)

    if isinstance(rating, int) and not isinstance(rating, bool):
        if rating <= LOW_RATING_MAX and reason is None:
            reasons.append(REASON_MISSING_COMPLAINT_REASON)

    return reasons


def is_valid(event: Mapping[str, Any], now: datetime | None = None) -> bool:
    """Atajo booleano sobre :func:`validation_reasons`."""
    return not validation_reasons(event, now)


def _functions():
    """Importa pyspark recien cuando se arma el plan; las pruebas puras no lo necesitan."""
    from pyspark.sql import functions as F  # noqa: PLC0415

    return F


def rejection_reason_column(now: datetime | None = None) -> Any:
    """Expresion con los motivos de rechazo separados por `;`. Vacio o null = valido."""
    F = _functions()
    now = now or datetime.now(timezone.utc)

    missing = [
        F.when(F.col(field).isNull(), F.lit(f"{REASON_MISSING_PREFIX}:{field}"))
        for field in REQUIRED_FIELDS
    ]
    future_limit = F.lit(now).cast("timestamp") + F.expr(
        f"INTERVAL {int(MAX_FUTURE_SKEW.total_seconds())} SECONDS"
    )

    checks = [
        # nullif: concat_ws devuelve "" cuando no falta nada, y un "" en la lista
        # externa dejaria el motivo con un ";" adelante.
        F.nullif(F.concat_ws(",", *missing), F.lit("")),
        F.when(F.col("event_type") != EVENT_TYPE, F.lit(REASON_INVALID_EVENT_TYPE)),
        F.when(
            F.col("rating").isNotNull()
            & ~F.col("rating").between(RATING_MIN, RATING_MAX),
            F.lit(REASON_RATING_OUT_OF_RANGE),
        ),
        F.when(
            (F.col("occurred_at").isNotNull()) & F.col("event_ts").isNull(),
            F.lit(REASON_INVALID_OCCURRED_AT),
        ),
        F.when(F.col("event_ts") > future_limit, F.lit(REASON_FUTURE_OCCURRED_AT)),
        F.when(
            F.col("channel").isNotNull() & ~F.col("channel").isin(*CHANNELS),
            F.lit(REASON_INVALID_CHANNEL),
        ),
        F.when(
            F.col("complaint_reason").isNotNull()
            & ~F.col("complaint_reason").isin(*COMPLAINT_REASONS),
            F.lit(REASON_INVALID_COMPLAINT_REASON),
        ),
        F.when(
            (F.col("rating") <= LOW_RATING_MAX) & F.col("complaint_reason").isNull(),
            F.lit(REASON_MISSING_COMPLAINT_REASON),
        ),
    ]
    return F.nullif(F.concat_ws(";", *checks), F.lit(""))


def transform_plan(now: datetime | None = None) -> list[tuple[str, Any]]:
    """Las transformaciones del Issue #8 como pares (columna, expresion).

    Se devuelven como pares en vez de una lista de columnas para que el job las aplique
    con `withColumn` de a una: asi `complaint_reason` se reemplaza en su lugar y no
    quedan dos columnas con el mismo nombre.
    """
    F = _functions()
    now = now or datetime.now(timezone.utc)

    rating_class_col = (
        F.when(F.col("rating") <= 2, F.lit("critico"))
        .when(F.col("rating") == LOW_RATING_MAX, F.lit("medio"))
        .when(F.col("rating") >= HIGH_RATING_MIN, F.lit("bueno"))
    )
    region_col = F.regexp_extract(F.col("zone_id"), r"^Z-([A-Za-z]+)$", 1)
    region_col = F.when(
        F.lower(region_col).isin(*ZONE_REGIONS), F.lower(region_col)
    )
    reason_col = (
        F.when(F.col("rating") >= HIGH_RATING_MIN, F.lit(None))
        .otherwise(F.col("complaint_reason"))
        .cast("string")
    )
    return [
        # 1. timestamp normalizado + columnas de particion
        ("dt", F.date_format(F.col("event_ts"), "yyyy-MM-dd")),
        ("hour", F.hour(F.col("event_ts"))),
        # 2. clasificacion del rating
        ("rating_class", rating_class_col),
        # 3. enriquecimiento con zona
        ("zone_region", region_col),
        # 4. motivo normalizado (regla 6 del contrato) y bandera para los KPI
        ("complaint_reason", reason_col),
        ("has_complaint", reason_col.isNotNull()),
        ("ingested_at", F.lit(now).cast("timestamp")),
    ]
