"""Contexto y comandos de una corrida orquestada por Airflow (Issue #15).

Este modulo no importa `airflow`: el DAG lo llama, pero la logica de que comando se
ejecuta y con que rutas se puede probar con `pytest` sin levantar nada. Tambien es el
unico lugar que traduce el `RunContext` a lineas de comando, para que el DAG no tenga
rutas ni flags hardcodeados.

Aislamiento por corrida: cada corrida usa su propio topic, lake y checkpoints. Sin eso,
`--once` con `startingOffsets=earliest` sobre el topic principal reprocesa el historico
completo y los conteos del reporte dejan de ser reproducibles. `scripts/verify_pipeline.py`
hizo la misma eleccion con `rapidgo.ratings.verify.<id>`.
"""

from __future__ import annotations

import os
import subprocess
import sys
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Mapping, Sequence

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_LAKE = REPO_ROOT / "datalake" / "runs"
DEFAULT_CHECKPOINTS = REPO_ROOT / "checkpoints" / "runs"
DEFAULT_BOOTSTRAP = "kafka:29092"
TOPIC_PREFIX = "rapidgo.ratings.run"

# Mismo conector que scripts/run_spark_job.py; si cambia alla, cambia aca.
KAFKA_CONNECTOR = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1"
SPARK_MASTER = "local[*]"

INGEST_JOB = "spark/streaming_job.py"
ALERTS_JOB = "spark/alerts_job.py"
KPI_JOB = "spark/kpis_job.py"


@dataclass(frozen=True)
class RunContext:
    """Todo lo que identifica y ubica una corrida. Se pasa entre tasks por XCom."""

    run_id: str
    topic: str
    lake_dir: str
    checkpoint_dir: str
    bootstrap_servers: str

    def as_xcom(self) -> dict[str, str]:
        return asdict(self)

    @classmethod
    def from_xcom(cls, payload: Mapping[str, str]) -> "RunContext":
        return cls(**dict(payload))

    @property
    def lake_path(self) -> Path:
        return Path(self.lake_dir)

    @property
    def checkpoint_path(self) -> Path:
        return Path(self.checkpoint_dir)


def make_run_id(now: datetime | None = None) -> str:
    """Identificador de corrida: fecha UTC compacta, valido en nombre de topic."""
    moment = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    return moment.strftime("%Y%m%dT%H%M%SZ")


def build_context(
    now: datetime | None = None,
    run_id: str | None = None,
    lake_root: str | Path | None = None,
    checkpoint_root: str | Path | None = None,
    bootstrap_servers: str | None = None,
    topic: str | None = None,
) -> RunContext:
    """Arma el contexto de una corrida nueva.

    Prioridad: argumento explicito, variable de entorno, default del repo. El `run_id`
    se usa como sufijo de topic y como carpeta, asi que dos corridas no se pisan.
    """
    identifier = run_id or make_run_id(now)
    lake_base = Path(
        lake_root or os.environ.get("RAPIDGO_LAKE_ROOT", DEFAULT_LAKE)
    ) / identifier
    checkpoint_base = Path(
        checkpoint_root or os.environ.get("RAPIDGO_CHECKPOINT_ROOT", DEFAULT_CHECKPOINTS)
    ) / identifier
    return RunContext(
        run_id=identifier,
        topic=topic or os.environ.get("RAPIDGO_TOPIC", f"{TOPIC_PREFIX}.{identifier}"),
        lake_dir=str(lake_base),
        checkpoint_dir=str(checkpoint_base),
        bootstrap_servers=bootstrap_servers
        or os.environ.get("RAPIDGO_BOOTSTRAP", DEFAULT_BOOTSTRAP),
    )


def python() -> str:
    """Interprete actual, salvo que el entorno fije uno (la imagen de Airflow)."""
    return os.environ.get("RAPIDGO_PYTHON", sys.executable)


def topic_command(ctx: RunContext, python_bin: str | None = None) -> list[str]:
    """Comando que crea el topic de la corrida segun ADR-001."""
    return [
        python_bin or python(),
        "-m",
        "scripts.create_topic",
        "--topic",
        ctx.topic,
        "--bootstrap-servers",
        ctx.bootstrap_servers,
    ]


def producer_command(
    ctx: RunContext,
    rate: float = 20.0,
    duration: float = 15.0,
    seed: int = 7,
    degradation: float = 0.7,
    dominant_reason: str = "late_delivery",
    backfill_minutes: float = 0.0,
    python_bin: str | None = None,
) -> list[str]:
    """Comando que publica los eventos de la corrida en su topic."""
    return [
        python_bin or python(),
        "-m",
        "producer.rating_producer",
        "--topic",
        ctx.topic,
        "--bootstrap-servers",
        ctx.bootstrap_servers,
        "--rate",
        _number(rate),
        "--duration",
        _number(duration),
        "--seed",
        str(seed),
        "--degradation",
        _number(degradation),
        "--dominant-reason",
        dominant_reason,
        "--backfill-minutes",
        _number(backfill_minutes),
    ]


def spark_job_command(
    ctx: RunContext,
    job: str,
    job_args: Sequence[str] = (),
    *,
    streaming: bool = True,
    python_bin: str | None = None,
) -> list[str]:
    """Comando que corre un job de PySpark contra el lake de la corrida.

    Un job de streaming consume el topic y guarda checkpoints; uno batch (los KPIs)
    solo lee el lake. Pasarle `--topic` al batch lo hace fallar con "unrecognized
    arguments", asi que el tipo de job decide que flags se agregan.

    Los jobs aceptan `--once`: procesan lo disponible y terminan, que es lo que un DAG
    batch necesita. `--master` no va aca: lo pone `spark_env()` por `PYSPARK_SUBMIT_ARGS`.
    """
    command = [
        python_bin or python(),
        str(REPO_ROOT / job),
        "--lake-dir",
        ctx.lake_dir,
    ]
    if streaming:
        command += ["--topic", ctx.topic, "--checkpoint-dir", ctx.checkpoint_dir]
    return command + list(job_args)


def spark_env(base: Mapping[str, str] | None = None) -> dict[str, str]:
    """Entorno que le dice a PySpark donde estan Java y el conector de Kafka.

    `PYSPARK_SUBMIT_ARGS` es el unico mecanismo que no exige el tarball de Spark: la
    imagen trae el wheel de PySpark y Java 17, y el conector se resuelve con Ivy. La
    primera corrida necesita red; el volumen `airflow-ivy` cachea las descargas.
    """
    env = dict(os.environ if base is None else base)
    env.setdefault("JAVA_HOME", "/usr/lib/jvm/java-17-openjdk-amd64")
    env.setdefault(
        "PYSPARK_SUBMIT_ARGS",
        f"--master {SPARK_MASTER} --packages {KAFKA_CONNECTOR} pyspark-shell",
    )
    env.setdefault("PYTHONUNBUFFERED", "1")
    return env


def _number(value: float) -> str:
    """20.0 -> '20', 0.7 -> '0.7': argumentos limpios en el log del task."""
    return str(int(value)) if float(value).is_integer() else str(value)


def run_command(
    command: Sequence[str],
    env: Mapping[str, str] | None = None,
    cwd: str | Path | None = None,
) -> int:
    """Ejecuta el comando con la salida heredada (va al log del task) y exige exito."""
    printable = " ".join(command)
    print(f"EJECUTANDO {printable}", flush=True)
    result = subprocess.run(
        list(command),
        cwd=str(cwd or REPO_ROOT),
        env=dict(env) if env is not None else None,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"el comando fallo con exit code {result.returncode}: {printable}"
        )
    return result.returncode
