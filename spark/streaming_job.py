"""Job de ingesta: Kafka `rapidgo.ratings` -> data lake en Parquet (Issues #8 y #9).

Corre en el contenedor de Spark (ADR-002), no en el host:

    python -m scripts.run_spark_job spark/streaming_job.py -- --duration 60
    python -m scripts.run_spark_job spark/streaming_job.py -- --once

Que hace por cada micro-batch:

- parsea el JSON con esquema explicito y normaliza `occurred_at` a timestamp UTC;
- clasifica el rating en `critico` / `medio` / `bueno`;
- deriva `zone_region` de `zone_id`;
- descarta los eventos que violan el contrato y los escribe en
  `datalake/curated/ratings_rejected` con el motivo;
- deduplica por `event_id` con watermark;
- escribe `datalake/raw/dt=.../hour=...` y `datalake/processed/dt=...` en Parquet.

Los checkpoints viven en `checkpoints/`, fuera del lake: reiniciar el job con los mismos
checkpoints no reprocesa lo ya escrito.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

# spark-submit arranca el script con su propia carpeta en sys.path[0]; la raiz del repo
# (donde vive el paquete `spark`) no queda importable sola.
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from spark.transformations import (  # noqa: E402
    EVENT_SCHEMA_SQL,
    TOPIC,
    WATERMARK,
    rejection_reason_column,
    transform_plan,
)

DEFAULT_LAKE = REPO_ROOT / "datalake"
DEFAULT_CHECKPOINTS = REPO_ROOT / "checkpoints"
KAFKA_BOOTSTRAP = "kafka:29092"

REJECTED_COLUMNS = (
    "event_id",
    "order_id",
    "zone_id",
    "channel",
    "rating",
    "complaint_reason",
    "occurred_at",
    "event_ts",
    "kafka_ts",
    "kafka_partition",
    "kafka_offset",
    "rejection_reason",
    "raw_value",
)


def _spark():
    from pyspark.sql import SparkSession  # noqa: PLC0415

    spark = (
        SparkSession.builder.appName("rapidgo-streaming-ingest")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_kafka(spark, args):
    """Stream de Kafka con el esquema del contrato aplicado al `value`."""
    from pyspark.sql import functions as F  # noqa: PLC0415

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap_servers)
        .option("subscribe", args.topic)
        .option("startingOffsets", args.starting_offsets)
        .option("failOnDataLoss", "false")
        .load()
    )
    return (
        raw.select(
            F.from_json(F.col("value").cast("string"), EVENT_SCHEMA_SQL).alias("payload"),
            F.col("timestamp").alias("kafka_ts"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            F.col("value").cast("string").alias("raw_value"),
        )
        .select("payload.*", "kafka_ts", "kafka_partition", "kafka_offset", "raw_value")
        # try_to_timestamp: con ANSI activado (default en Spark 4) un `occurred_at`
        # malformado lanzaria CAST_INVALID_INPUT en vez de quedar null. La regla 3 del
        # contrato pide descartar el evento, no abortar el batch.
        .withColumn("event_ts", F.try_to_timestamp(F.col("occurred_at")))
    )


def prepare(spark, args):
    """Separa validos de rechazados y aplica las transformaciones del contrato."""
    from pyspark.sql import functions as F  # noqa: PLC0415

    parsed = read_kafka(spark, args)
    classified = parsed.withColumn("rejection_reason", rejection_reason_column())

    valid = classified.where(F.col("rejection_reason").isNull()).drop(
        "rejection_reason", "raw_value"
    )
    for name, expression in transform_plan():
        valid = valid.withColumn(name, expression)
    deduplicated = valid.withWatermark("event_ts", WATERMARK).dropDuplicates(["event_id"])

    rejected = (
        classified.where(F.col("rejection_reason").isNotNull())
        .select(*REJECTED_COLUMNS)
        .withColumn("dt", F.date_format(F.col("kafka_ts"), "yyyy-MM-dd"))
    )
    return deduplicated, rejected


def write_raw_and_processed(batch_df, batch_id, lake: Path):
    """Escribe el batch una vez y lo reusa para las dos capas del lake."""
    if batch_df.isEmpty():
        print(f"BATCH {batch_id} validos=0", flush=True)
        return

    rows = batch_df.count()
    batch_df.persist()
    (
        batch_df.write.mode("append")
        .partitionBy("dt", "hour")
        .parquet(str(lake / "raw"))
    )
    batch_df.write.mode("append").partitionBy("dt").parquet(str(lake / "processed"))
    batch_df.unpersist()
    print(f"BATCH {batch_id} validos={rows}", flush=True)


def write_rejected(batch_df, batch_id, lake: Path):
    if batch_df.isEmpty():
        print(f"BATCH {batch_id} rechazados=0", flush=True)
        return
    rows = batch_df.count()
    batch_df.write.mode("append").parquet(str(lake / "curated" / "ratings_rejected"))
    print(f"BATCH {batch_id} rechazados={rows}", flush=True)


def lake_counts(spark, lake: Path) -> dict[str, int]:
    """Conteo por capa, para la evidencia de los Issues #8 y #9."""
    paths = {
        "raw": lake / "raw",
        "processed": lake / "processed",
        "rechazados": lake / "curated" / "ratings_rejected",
    }
    return {
        name: spark.read.parquet(str(path)).count() if path.exists() else 0
        for name, path in paths.items()
    }


def lake_summary(spark, lake: Path) -> list[str]:
    """Lineas de evidencia: arbol del lake, distribucion de clases y motivos de rechazo."""
    lines: list[str] = []
    raw_path = lake / "raw"
    if raw_path.exists():
        partitions = (
            spark.read.parquet(str(raw_path))
            .select("dt", "hour")
            .distinct()
            .orderBy("dt", "hour")
            .collect()
        )
        tree = ", ".join(f"dt={row.dt}/hour={row.hour}" for row in partitions)
        lines.append(f"ARBOL raw: {tree}")
    processed_path = lake / "processed"
    if processed_path.exists():
        rows = (
            spark.read.parquet(str(processed_path))
            .groupBy("rating_class")
            .count()
            .orderBy("rating_class")
            .collect()
        )
        lines.append(
            "CLASES " + ", ".join(f"{row.rating_class}={row['count']}" for row in rows)
        )
    rejected_path = lake / "curated" / "ratings_rejected"
    if rejected_path.exists():
        rows = (
            spark.read.parquet(str(rejected_path))
            .groupBy("rejection_reason")
            .count()
            .orderBy("rejection_reason")
            .collect()
        )
        lines.append(
            "MOTIVOS " + ", ".join(f"{row.rejection_reason}={row['count']}" for row in rows)
        )
    return lines


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Ingesta Kafka -> data lake (Issues #8 y #9).")
    parser.add_argument("--bootstrap-servers", default=KAFKA_BOOTSTRAP)
    parser.add_argument("--topic", default=TOPIC)
    parser.add_argument("--lake-dir", default=str(DEFAULT_LAKE))
    parser.add_argument("--checkpoint-dir", default=str(DEFAULT_CHECKPOINTS))
    parser.add_argument(
        "--starting-offsets",
        default="earliest",
        choices=("earliest", "latest"),
        help="solo aplica en la primera corrida; despues manda el checkpoint",
    )
    parser.add_argument("--trigger", default="5 seconds", help="intervalo de micro-batch")
    parser.add_argument(
        "--duration",
        type=float,
        default=60.0,
        help="segundos que queda escuchando antes de cerrar",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="procesa lo disponible y termina (availableNow), para la prueba de reinicio",
    )
    args = parser.parse_args(argv)

    lake = Path(args.lake_dir)
    checkpoints = Path(args.checkpoint_dir)
    spark = _spark()
    print(f"JOB lake={lake} checkpoints={checkpoints} topic={args.topic}", flush=True)

    valid, rejected = prepare(spark, args)
    valid_query = valid.writeStream.foreachBatch(
        lambda batch_df, batch_id: write_raw_and_processed(batch_df, batch_id, lake)
    ).option("checkpointLocation", str(checkpoints / "ingest_valid"))
    rejected_query = rejected.writeStream.foreachBatch(
        lambda batch_df, batch_id: write_rejected(batch_df, batch_id, lake)
    ).option("checkpointLocation", str(checkpoints / "ingest_rejected"))

    if args.once:
        valid_query = valid_query.trigger(availableNow=True)
        rejected_query = rejected_query.trigger(availableNow=True)
    else:
        valid_query = valid_query.trigger(processingTime=args.trigger)
        rejected_query = rejected_query.trigger(processingTime=args.trigger)

    active = [valid_query.start(), rejected_query.start()]
    try:
        for query in active:
            query.awaitTermination(None if args.once else args.duration)
    except KeyboardInterrupt:
        print("interrumpido por el usuario", file=sys.stderr, flush=True)
    finally:
        for query in active:
            if query.isActive:
                query.stop()

    counts = lake_counts(spark, lake)
    print(
        f"LAKE raw={counts['raw']} processed={counts['processed']} "
        f"rechazados={counts['rechazados']}",
        flush=True,
    )
    for line in lake_summary(spark, lake):
        print(line, flush=True)
    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
