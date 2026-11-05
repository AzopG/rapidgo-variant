"""Job batch de agregacion: calcula K1, K2 y K3 sobre `processed` (Issue #15).

El DAG de Airflow lo corre despues de la ingesta y de las alertas, con el lake de la
corrida. Es batch y no streaming a proposito: el DAG necesita un corte finito por
corrida para que el reporte de #16 tenga numeros que se puedan auditar.

Salidas, segun `docs/kpis-alerts.md:14-18`:

- `curated/kpi_rating_avg`: K1, `avg(rating)` por ventana y dimension.
- `curated/kpi_top_reasons`: K2, top 5 de motivos con su participacion.
- `curated/kpi_trend`: K3, ultimos 60 min contra los 60 anteriores.

Modo `overwrite`: un reintento de la task reemplaza la salida de la corrida en vez de
duplicarla. El lake es por corrida, asi que no pisa datos de otra.
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from spark.kpi_rules import (  # noqa: E402
    DIMENSIONS,
    GLOBAL_LABEL,
    K1_SLIDE,
    K1_WINDOW,
    K2_SLIDE,
    K2_WINDOW,
    K3_PREVIOUS_MINUTES,
    K3_WINDOW_MINUTES,
    TOP_REASONS,
    TREND_DOWN,
    TREND_STABLE,
    TREND_THRESHOLD,
    TREND_UP,
)
from spark.sources import DEFAULT_LAKE, spark_session  # noqa: E402

OUTPUTS = ("kpi_rating_avg", "kpi_top_reasons", "kpi_trend")


def _functions():
    """Importa pyspark recien cuando se arma el plan; las pruebas usan un doble."""
    from pyspark.sql import functions as F  # noqa: PLC0415

    return F


def k1_frame(events, dimensions=DIMENSIONS):
    """K1: media por ventana de 10 min, global y por zona, canal y repartidor."""
    F = _functions()
    window = F.window(F.col("event_ts"), K1_WINDOW, K1_SLIDE)
    frames = []
    for dimension in dimensions:
        if dimension == "global":
            scoped = events
            value = F.lit(GLOBAL_LABEL)
        else:
            # Una dimension opcional con null no se puede agregar: "sin dato" no es
            # una categoria del KPI.
            scoped = events.where(F.col(dimension).isNotNull())
            value = F.col(dimension)
        frames.append(
            scoped.groupBy(
                window.alias("window"), value.alias("dimension_value")
            )
            .agg(F.avg("rating").alias("value"), F.count("*").alias("n"))
            .withColumn("dimension", F.lit(dimension))
            .select("window", "dimension", "dimension_value", "value", "n")
        )
    result = frames[0]
    for frame in frames[1:]:
        result = result.unionByName(frame)
    return result.select(
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        "dimension",
        "dimension_value",
        "value",
        "n",
    )


def k2_frame(events):
    """K2: top 5 de motivos por ventana de 15 min, con participacion sobre reclamos."""
    from pyspark.sql import Window  # noqa: PLC0415

    F = _functions()
    complaints = events.where(F.col("complaint_reason").isNotNull())
    window = F.window(F.col("event_ts"), K2_WINDOW, K2_SLIDE)

    totals = complaints.groupBy(window.alias("window")).agg(
        F.count("*").alias("total_reclamos")
    )
    counts = complaints.groupBy(
        window.alias("window"), F.col("complaint_reason").alias("complaint_reason")
    ).agg(F.count("*").alias("n"))

    joined = counts.join(totals, on="window", how="inner").withColumn(
        "share", F.col("n") / F.col("total_reclamos")
    )
    ranked = joined.withColumn(
        "rank",
        F.row_number().over(
            Window.partitionBy("window").orderBy(
                F.desc("share"), F.asc("complaint_reason")
            )
        ),
    )
    return ranked.where(F.col("rank") <= TOP_REASONS).select(
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        "complaint_reason",
        "n",
        "total_reclamos",
        "share",
        "rank",
    )


def k3_frame(events, reference: datetime | None = None):
    """K3: media de los ultimos 60 min contra los 60 anteriores.

    `reference` es el borde derecho de la ventana actual; por defecto, el ultimo
    `event_ts` del lake. Una sola fila, con nulos explicitos si falta una de las dos
    mitades: el reporte de #16 necesita distinguir "sin datos" de "estable".
    """
    F = _functions()
    if reference is None:
        reference = events.agg(F.max("event_ts")).first()[0]
    if reference is None:
        raise ValueError("el lake no tiene eventos: no se puede calcular K3")

    now_start = reference - timedelta(minutes=K3_WINDOW_MINUTES)
    previous_start = now_start - timedelta(minutes=K3_PREVIOUS_MINUTES)
    previous_end = now_start

    def aggregate(start: datetime, end: datetime, suffix: str):
        scoped = events.where(
            (F.col("event_ts") > F.lit(start)) & (F.col("event_ts") <= F.lit(end))
        )
        return scoped.agg(
            F.avg("rating").alias(f"avg_{suffix}"),
            F.count("*").alias(f"n_{suffix}"),
        )

    now = aggregate(now_start, reference, "now")
    previous = aggregate(previous_start, previous_end, "previous")

    difference = F.col("avg_now") - F.col("avg_previous")
    state = (
        F.when(difference.isNull(), F.lit(None).cast("string"))
        .when(difference <= F.lit(-TREND_THRESHOLD), F.lit(TREND_DOWN))
        .when(difference >= F.lit(TREND_THRESHOLD), F.lit(TREND_UP))
        .otherwise(F.lit(TREND_STABLE))
    )
    percent = (
        F.when(
            F.col("avg_previous").isNull() | (F.col("avg_previous") == F.lit(0)),
            F.lit(None).cast("double"),
        ).otherwise(difference / F.col("avg_previous") * F.lit(100.0))
    )

    return now.crossJoin(previous).select(
        F.lit(now_start).cast("timestamp").alias("window_start"),
        F.lit(reference).cast("timestamp").alias("window_end"),
        F.lit(previous_start).cast("timestamp").alias("previous_start"),
        F.lit(previous_end).cast("timestamp").alias("previous_end"),
        "avg_now",
        "n_now",
        "avg_previous",
        "n_previous",
        difference.alias("delta"),
        percent.alias("pct_delta"),
        state.alias("state"),
    )


def read_processed(spark, lake: Path):
    path = lake / "processed"
    if not path.exists():
        raise FileNotFoundError(
            f"no existe {path}: el job de ingesta tiene que correr antes que el de KPIs"
        )
    return spark.read.parquet(str(path))


def write_kpis(spark, lake: Path, events, reference: datetime | None = None) -> dict[str, int]:
    """Calcula y persiste los tres KPIs. Devuelve el conteo por salida."""
    curated = lake / "curated"
    frames = {
        "kpi_rating_avg": k1_frame(events),
        "kpi_top_reasons": k2_frame(events),
        "kpi_trend": k3_frame(events, reference),
    }
    counts: dict[str, int] = {}
    for name, frame in frames.items():
        rows = frame.count()
        frame.write.mode("overwrite").parquet(str(curated / name))
        counts[name] = rows
        print(f"KPI {name} filas={rows}", flush=True)
    return counts


def summarize(spark, lake: Path) -> list[str]:
    """Lineas de evidencia para el log del task: los valores que el reporte va a citar."""
    F = _functions()
    lines: list[str] = []
    trend_path = lake / "curated" / "kpi_trend"
    if trend_path.exists():
        row = spark.read.parquet(str(trend_path)).first()
        if row is not None:
            lines.append(
                f"KPI K3 avg_now={row.avg_now} avg_previous={row.avg_previous} "
                f"delta={row.delta} state={row.state}"
            )
    avg_path = lake / "curated" / "kpi_rating_avg"
    if avg_path.exists():
        row = (
            spark.read.parquet(str(avg_path))
            .where(F.col("dimension") == GLOBAL_LABEL)
            .orderBy(F.col("window_end").desc())
            .first()
        )
        if row is not None:
            lines.append(f"KPI K1 ultima_ventana avg={row.value:.4f} n={row.n}")
    return lines


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Calcula los tres KPIs sobre el lake (Issue #15)."
    )
    parser.add_argument("--lake-dir", default=str(DEFAULT_LAKE))
    parser.add_argument(
        "--reference",
        default=None,
        help="borde derecho ISO-8601 de la ventana de K3 (por defecto, el ultimo event_ts)",
    )
    parser.add_argument(
        "--lookback-minutes",
        type=float,
        default=K3_WINDOW_MINUTES + K3_PREVIOUS_MINUTES,
        help="ventana de datos que se lee para K1/K2 y para comparar K3",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    lake = Path(args.lake_dir)
    reference = (
        datetime.fromisoformat(args.reference.replace("Z", "+00:00"))
        if args.reference
        else None
    )

    spark = spark_session("rapidgo-kpis")
    try:
        events = read_processed(spark, lake)
        if reference is None:
            reference = events.agg(_functions().max("event_ts")).first()[0]
        if reference is None:
            print("ERROR: el lake de processed esta vacio", file=sys.stderr)
            return 1
        lookback = reference - timedelta(minutes=args.lookback_minutes)
        scoped = events.where(_functions().col("event_ts") >= lookback)
        counts = write_kpis(spark, lake, scoped, reference)
        print(f"KPIS total={sum(counts.values())}", flush=True)
        for line in summarize(spark, lake):
            print(line, flush=True)
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
