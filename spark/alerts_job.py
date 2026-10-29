"""Job de alertas: A1 (caida de calificacion) y A2 (concentracion de reclamos).

Issues #10 y #11. Corre en el contenedor de Spark (ADR-002):

    python -m scripts.run_spark_job spark/alerts_job.py -- --duration 60
    python -m scripts.run_spark_job spark/alerts_job.py -- --once

Ventanas por `occurred_at`, no por hora de llegada. Las dos alertas se evaluan global y
por `zone_id` y se escriben en `datalake/curated/alerts` con el valor observado, el
umbral que las disparo y la ventana, para poder auditar por que sono.

Las dos queries escriben a la misma salida, asi que la persistencia pasa por
`spark/alerts_writer.py`: Spark no admite dos committers concurrentes sobre una misma
ruta y sin serializar se pierden filas con `ParentNotDirectoryException`.

Cooldown: una alerta por `(alerta, zona, motivo)` cada 15 minutos. Se mide contra el
`window_end` de la ventana que la disparo, asi que dos ventanas consecutivas de 10
minutos no generan dos alertas de la misma condicion sostenida. Al arrancar, el
cooldown se siembra desde `curated/alerts`, de modo que un reinicio no repite la alerta.
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

# spark-submit arranca el script con su propia carpeta en sys.path[0].
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from pyspark.sql import types as T  # noqa: E402

from spark.alert_rules import (  # noqa: E402
    A1_WINDOW,
    A2_WINDOW,
    GLOBAL_ZONE,
    a1_severity,
    a2_severity,
    cooldown_allows,
    cooldown_key,
)
from spark.alerts_writer import append_alerts  # noqa: E402
from spark.sources import (  # noqa: E402
    DEFAULT_CHECKPOINTS,
    DEFAULT_LAKE,
    KAFKA_BOOTSTRAP,
    read_kafka,
    spark_session,
)
from spark.transformations import TOPIC, WATERMARK, rejection_reason_column  # noqa: E402

ALERT_SCHEMA = T.StructType(
    [
        T.StructField("alert_id", T.StringType()),
        T.StructField("severity", T.StringType()),
        T.StructField("rule_window", T.StringType()),
        T.StructField("window_start", T.TimestampType()),
        T.StructField("window_end", T.TimestampType()),
        T.StructField("zone_id", T.StringType()),
        T.StructField("complaint_reason", T.StringType()),
        T.StructField("observed_value", T.DoubleType()),
        T.StructField("threshold", T.DoubleType()),
        T.StructField("sample_size", T.LongType()),
        T.StructField("evaluated_at", T.TimestampType()),
    ]
)


def alerts_path(lake: Path) -> Path:
    return lake / "curated" / "alerts"


def seed_cooldown(spark, lake: Path) -> dict[tuple, datetime]:
    """Recupera el ultimo `window_end` por clave para que un reinicio respete el cooldown."""
    from pyspark.sql import functions as F  # noqa: PLC0415

    path = alerts_path(lake)
    if not path.exists():
        return {}
    rows = (
        spark.read.parquet(str(path))
        .groupBy("alert_id", "zone_id", "complaint_reason")
        .agg(F.max("window_end").alias("last_window_end"))
        .collect()
    )
    return {
        cooldown_key(row.alert_id, row.zone_id, row.complaint_reason): row.last_window_end
        for row in rows
    }


def _emit(spark, lake: Path, records: list[tuple]) -> None:
    """Persiste las alertas del batch. Serializado: A1 y A2 comparten la salida."""
    append_alerts(spark, alerts_path(lake), records, ALERT_SCHEMA)


def write_a1(batch_df, batch_id, lake: Path, state: dict[tuple, datetime]) -> None:
    """Evalua A1 sobre la ventana y persiste solo lo que paso el cooldown."""
    spark = batch_df.sparkSession
    now = datetime.now(timezone.utc)
    records: list[tuple] = []
    evaluated = 0

    for row in batch_df.collect():
        decision = a1_severity(row.avg_rating, row.n)
        if decision is None:
            continue
        evaluated += 1
        moment = row.window["end"]
        key = cooldown_key(decision.alert_id, row.zone_id, None)
        if not cooldown_allows(key, moment, state):
            continue
        state[key] = moment
        records.append(
            (
                decision.alert_id,
                decision.severity,
                A1_WINDOW,
                row.window["start"],
                moment,
                row.zone_id,
                None,
                float(row.avg_rating),
                decision.threshold,
                int(row.n),
                now,
            )
        )

    _emit(spark, lake, records)
    print(
        f"A1 BATCH {batch_id} evaluadas={evaluated} alertas={len(records)}", flush=True
    )


def write_a2(batch_df, batch_id, lake: Path, state: dict[tuple, datetime]) -> None:
    """Evalua A2 sobre la ventana, global y por zona, con cooldown por motivo."""
    from spark.transformations import COMPLAINT_REASONS  # noqa: PLC0415

    spark = batch_df.sparkSession
    now = datetime.now(timezone.utc)
    records: list[tuple] = []
    evaluated = 0

    for row in batch_df.collect():
        total = int(row.total_reclamos)
        if total == 0:
            continue
        moment = row.window["end"]
        for index, reason in enumerate(COMPLAINT_REASONS):
            n_reclamos = int(row[reason_column(index)] or 0)
            share = n_reclamos / total
            decision = a2_severity(share, n_reclamos)
            if decision is None:
                continue
            evaluated += 1
            key = cooldown_key(decision.alert_id, row.zone_id, reason)
            if not cooldown_allows(key, moment, state):
                continue
            state[key] = moment
            records.append(
                (
                    decision.alert_id,
                    decision.severity,
                    A2_WINDOW,
                    row.window["start"],
                    moment,
                    row.zone_id,
                    reason,
                    float(share),
                    decision.threshold,
                    n_reclamos,
                    now,
                )
            )

    _emit(spark, lake, records)
    print(
        f"A2 BATCH {batch_id} evaluadas={evaluated} alertas={len(records)}", flush=True
    )


def valid_events(spark, args):
    """Eventos que pasan el contrato: un rechazado no puede mover un promedio."""
    from pyspark.sql import functions as F  # noqa: PLC0415

    parsed = read_kafka(
        spark,
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        starting_offsets=args.starting_offsets,
    )
    return (
        parsed.withColumn("rejection_reason", rejection_reason_column())
        .where(F.col("rejection_reason").isNull())
        .drop("rejection_reason", "raw_value")
        .withWatermark("event_ts", WATERMARK)
    )


def scoped_for_aggregation(valid):
    """Agrega una copia etiquetada como GLOBAL: una sola agregacion cubre los dos niveles."""
    from pyspark.sql import functions as F  # noqa: PLC0415

    return valid.unionByName(valid.withColumn("zone_id", F.lit(GLOBAL_ZONE)))


def a1_stream(valid):
    from pyspark.sql import functions as F  # noqa: PLC0415

    scoped = scoped_for_aggregation(valid)
    return scoped.groupBy(F.window(F.col("event_ts"), A1_WINDOW), F.col("zone_id")).agg(
        F.avg("rating").alias("avg_rating"), F.count("*").alias("n")
    )


def a2_stream(valid):
    """Conteos por motivo en una sola agregacion.

    En `update` mode Spark no soporta el join entre dos streams, asi que la
    participacion se calcula con conteos condicionales por motivo (la enumeracion del
    contrato es cerrada) en vez de joinear el total contra el conteo por motivo.
    """
    from pyspark.sql import functions as F  # noqa: PLC0415

    from spark.transformations import COMPLAINT_REASONS  # noqa: PLC0415

    complaints = valid.where(F.col("complaint_reason").isNotNull())
    scoped = scoped_for_aggregation(complaints)
    aggregates = [F.count("*").alias("total_reclamos")]
    for index, reason in enumerate(COMPLAINT_REASONS):
        aggregates.append(
            F.sum(F.when(F.col("complaint_reason") == reason, 1).otherwise(0)).alias(
                reason_column(index)
            )
        )
    return scoped.groupBy(F.window(F.col("event_ts"), A2_WINDOW), F.col("zone_id")).agg(
        *aggregates
    )


def reason_column(index: int) -> str:
    """Nombre de la columna de conteo del motivo `index` de la enumeracion."""
    return f"n_motivo_{index}"


def print_alerts(spark, lake: Path) -> None:
    """Resumen de lo persistido, para la evidencia de los Issues #10 y #11."""
    from pyspark.sql import functions as F  # noqa: PLC0415

    path = alerts_path(lake)
    if not path.exists():
        print("ALERTAS total=0", flush=True)
        return
    frame = spark.read.parquet(str(path))
    print(f"ALERTAS total={frame.count()}", flush=True)
    rows = (
        frame.groupBy("alert_id", "severity", "zone_id")
        .agg(F.count("*").alias("n"), F.max("observed_value").alias("max_valor"))
        .orderBy("alert_id", "severity", "zone_id")
        .collect()
    )
    for row in rows:
        print(
            f"ALERTA {row.alert_id} {row.severity} zona={row.zone_id} "
            f"n={row.n} max_valor={row.max_valor:.4f}",
            flush=True,
        )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Alertas A1 y A2 (Issues #10 y #11).")
    parser.add_argument("--bootstrap-servers", default=KAFKA_BOOTSTRAP)
    parser.add_argument("--topic", default=TOPIC)
    parser.add_argument("--lake-dir", default=str(DEFAULT_LAKE))
    parser.add_argument("--checkpoint-dir", default=str(DEFAULT_CHECKPOINTS))
    parser.add_argument(
        "--starting-offsets", default="earliest", choices=("earliest", "latest")
    )
    parser.add_argument("--trigger", default="5 seconds")
    parser.add_argument("--duration", type=float, default=90.0)
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args(argv)

    lake = Path(args.lake_dir)
    checkpoints = Path(args.checkpoint_dir)
    spark = spark_session("rapidgo-streaming-alerts")
    print(f"JOB alertas lake={lake} checkpoints={checkpoints} topic={args.topic}", flush=True)

    state = seed_cooldown(spark, lake)
    print(f"COOLDOWN claves_sembradas={len(state)}", flush=True)

    valid = valid_events(spark, args)
    a1_query = a1_stream(valid).writeStream.foreachBatch(
        lambda batch_df, batch_id: write_a1(batch_df, batch_id, lake, state)
    ).outputMode("update").option("checkpointLocation", str(checkpoints / "alerts_a1"))
    a2_query = a2_stream(valid).writeStream.foreachBatch(
        lambda batch_df, batch_id: write_a2(batch_df, batch_id, lake, state)
    ).outputMode("update").option("checkpointLocation", str(checkpoints / "alerts_a2"))

    if args.once:
        a1_query = a1_query.trigger(availableNow=True)
        a2_query = a2_query.trigger(availableNow=True)
    else:
        a1_query = a1_query.trigger(processingTime=args.trigger)
        a2_query = a2_query.trigger(processingTime=args.trigger)

    active = [a1_query.start(), a2_query.start()]
    try:
        for query in active:
            query.awaitTermination(None if args.once else args.duration)
    except KeyboardInterrupt:
        print("interrumpido por el usuario", file=sys.stderr, flush=True)
    finally:
        for query in active:
            if query.isActive:
                query.stop()

    print_alerts(spark, lake)
    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
