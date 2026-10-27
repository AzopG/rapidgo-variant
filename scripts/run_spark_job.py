"""Ejecuta un job de PySpark en el contenedor oficial, segun ADR-002.

En Windows, Spark no puede escribir Parquet sin `winutils.exe`, y el conector de Kafka
falla por la misma causa. Por eso el job corre en un contenedor Linux y solo el producer
queda en el host.

Uso:
    python -m scripts.run_spark_job spark/streaming_job.py
    python -m scripts.run_spark_job spark/streaming_job.py -- --duration 60
    python -m scripts.run_spark_job spark/streaming_job.py --dry-run

Requisitos: `docker compose up -d kafka` y el topic creado con
`python -m scripts.create_topic`.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

SPARK_IMAGE = "apache/spark:4.0.1"
KAFKA_CONNECTOR = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1"
KAFKA_CONTAINER = "rapidgo-kafka"
CONTAINER_REPO = "/opt/rapidgo"
IVY_DIR = "/tmp/.ivy2"
SPARK_SUBMIT = "/opt/spark/bin/spark-submit"


def detect_network(kafka_container: str = KAFKA_CONTAINER) -> str:
    """Devuelve la red de Docker a la que esta conectado el broker."""
    result = subprocess.run(
        [
            "docker",
            "inspect",
            kafka_container,
            "--format",
            "{{range $name, $net := .NetworkSettings.Networks}}{{$name}}{{end}}",
        ],
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    network = (result.stdout or "").strip()
    if result.returncode != 0 or not network:
        raise RuntimeError(
            f"no se encontro la red de {kafka_container}. "
            f"Ejecuta 'docker compose up -d kafka' primero."
        )
    return network


def build_command(
    repo_root: Path,
    job: str,
    network: str,
    job_args: list[str] | None = None,
    image: str = SPARK_IMAGE,
    connector: str = KAFKA_CONNECTOR,
) -> list[str]:
    """Construye el `docker run ... spark-submit ...` completo."""
    job_path = f"{CONTAINER_REPO}/{job.replace(chr(92), '/')}"
    command = [
        "docker",
        "run",
        "--rm",
        "--network",
        network,
        "-v",
        f"{repo_root}:{CONTAINER_REPO}",
        image,
        SPARK_SUBMIT,
        "--packages",
        connector,
        "--conf",
        f"spark.jars.ivy={IVY_DIR}",
        "--master",
        "local[2]",
        "--conf",
        "spark.sql.shuffle.partitions=2",
        job_path,
    ]
    return command + list(job_args or [])


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Ejecuta un job de PySpark en el contenedor de ADR-002."
    )
    parser.add_argument("job", help="ruta del job relativa a la raiz del repo")
    parser.add_argument("--network", default=None, help="red de Docker (por defecto, la de Kafka)")
    parser.add_argument("--image", default=SPARK_IMAGE)
    parser.add_argument("--dry-run", action="store_true", help="muestra el comando y sale")
    args, job_args = parser.parse_known_args(argv)
    if job_args and job_args[0] == "--":
        job_args = job_args[1:]

    repo_root = Path(__file__).resolve().parent.parent
    job_file = (repo_root / args.job).resolve()
    if repo_root not in job_file.parents:
        print("ERROR: el job debe estar dentro del repositorio", file=sys.stderr)
        return 2

    try:
        network = args.network or detect_network()
    except RuntimeError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 3

    command = build_command(
        repo_root, args.job, network, job_args, image=args.image
    )
    if args.dry_run:
        print(" ".join(command))
        return 0

    if not job_file.is_file():
        print(f"ERROR: no existe el job {job_file}", file=sys.stderr)
        return 2

    print(f"ejecutando {args.job} en {args.image} (red {network})")
    return subprocess.call(command)


if __name__ == "__main__":
    raise SystemExit(main())
