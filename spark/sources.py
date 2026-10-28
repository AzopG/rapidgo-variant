"""Sesion de Spark y lectura del topic compartidas por los jobs.

Ambos jobs (`streaming_job.py` y `alerts_job.py`) leen el mismo topic con el mismo
esquema y la misma normalizacion de `occurred_at`; tenerlo en un solo lugar evita que
uno de los dos se desincronice del contrato.
"""

from __future__ import annotations

from pathlib import Path

from spark.transformations import EVENT_SCHEMA_SQL, TOPIC

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_LAKE = REPO_ROOT / "datalake"
DEFAULT_CHECKPOINTS = REPO_ROOT / "checkpoints"
KAFKA_BOOTSTRAP = "kafka:29092"


def spark_session(app_name: str):
    """Sesion en UTC: las particiones `dt`/`hour` no pueden depender de la zona del host."""
    from pyspark.sql import SparkSession  # noqa: PLC0415

    spark = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_kafka(spark, bootstrap_servers: str = KAFKA_BOOTSTRAP, topic: str = TOPIC,
               starting_offsets: str = "earliest"):
    """Stream de Kafka con el esquema del contrato aplicado al `value`."""
    from pyspark.sql import functions as F  # noqa: PLC0415

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", starting_offsets)
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
