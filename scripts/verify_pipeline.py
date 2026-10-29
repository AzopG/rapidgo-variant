"""Verificacion end-to-end del pipeline de la Semana 2 (Issue #33).

Un solo comando que demuestra que producer -> Kafka -> Spark -> Data Lake corre de
punta a punta y deja la evidencia para el checkpoint, en vez de repetir a mano los
pasos de #31 y #32.

Pasos de la corrida completa:

1. ``docker compose up -d --wait kafka``
2. crea un topic propio de la corrida (``rapidgo.ratings.verify.<id>``) con
   ``scripts.create_topic``: asi los conteos no se mezclan con datos viejos del topic
   principal;
3. publica eventos degradados con ``producer.rating_producer``;
4. inyecta un evento invalido por cada regla de rechazo de ``docs/event-contract.md``
   (el producer solo genera eventos validos, y sin esto ``rechazados`` siempre seria 0);
5. corre ``spark/streaming_job.py --once`` y ``spark/alerts_job.py --once`` sobre un
   lake temporal en ``.verify/<id>/`` (dentro del repo, porque el contenedor de Spark
   solo ve el repo montado en ``/opt/rapidgo``);
6. verifica: ``raw == processed == publicados``, ``rechazados == inyectados > 0``,
   ``curated/alerts`` no vacio y las particiones ``dt=/hour=`` en disco.

Uso:
    python -m scripts.verify_pipeline                    # corrida completa (Docker)
    python -m scripts.verify_pipeline --structure-only   # sin Docker, para CI

Codigos de salida:
    0  todo verificado
    1  el pipeline corrio pero los conteos o el lake no cumplen
    2  estructura del repo o argumentos invalidos
    3  un paso de la corrida fallo (Docker, Kafka, producer o Spark)

No modifica ``spark/`` ni ``producer/``: los usa tal como estan.
"""

from __future__ import annotations

import argparse
import ast
import json
import os
import re
import shutil
import subprocess
import sys
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping

REPO_ROOT = Path(__file__).resolve().parents[1]
CONTAINER_REPO = "/opt/rapidgo"  # mismo valor que scripts/run_spark_job.py
VERIFY_DIR = ".verify"
TOPIC_PREFIX = "rapidgo.ratings.verify"
DEFAULT_BOOTSTRAP = "localhost:9092"

INGEST_JOB = "spark/streaming_job.py"
ALERTS_JOB = "spark/alerts_job.py"
JOBS = (INGEST_JOB, ALERTS_JOB)
# Flags que el script les pasa a los jobs. Si alguno desaparece, la corrida no sirve.
REQUIRED_JOB_FLAGS = ("--once", "--topic", "--lake-dir", "--checkpoint-dir")

REQUIRED_PATHS = (
    "docker-compose.yml",
    "requirements.txt",
    "samples/rating_event.json",
    "docs/event-contract.md",
    "producer/rating_producer.py",
    "scripts/create_topic.py",
    "scripts/run_spark_job.py",
    "spark/sources.py",
    "spark/transformations.py",
    INGEST_JOB,
    ALERTS_JOB,
)

EXIT_OK = 0
EXIT_VERIFICATION = 1
EXIT_STRUCTURE = 2
EXIT_STEP = 3

LAKE_RE = re.compile(r"^LAKE raw=(\d+) processed=(\d+) rechazados=(\d+)", re.MULTILINE)
ALERTS_RE = re.compile(r"^ALERTAS total=(\d+)", re.MULTILINE)
PUBLISHED_RE = re.compile(r"publicados=(\d+)")


class VerificationError(RuntimeError):
    """La salida de un paso no tiene el formato esperado."""


@dataclass(frozen=True)
class RunConfig:
    """Parametros de una corrida. Todo lo que no es Docker se puede probar con esto."""

    repo_root: Path
    run_id: str
    rate: float = 20.0
    duration: float = 15.0
    seed: int = 7
    degradation: float = 0.7
    dominant_reason: str = "late_delivery"
    bootstrap_servers: str = DEFAULT_BOOTSTRAP
    python: str = sys.executable

    @property
    def topic(self) -> str:
        return f"{TOPIC_PREFIX}.{self.run_id}"

    @property
    def run_dir(self) -> Path:
        return self.repo_root / VERIFY_DIR / self.run_id

    @property
    def lake_dir(self) -> Path:
        return self.run_dir / "lake"

    @property
    def checkpoint_dir(self) -> Path:
        return self.run_dir / "checkpoints"


@dataclass(frozen=True)
class Step:
    """Un paso de la corrida: nombre legible y el comando que lo ejecuta."""

    name: str
    argv: tuple[str, ...]


@dataclass
class Observed:
    """Lo que la corrida dejo a la vista, para evaluar y para el resumen."""

    published: int = 0
    injected: int = 0
    raw: int = 0
    processed: int = 0
    rejected: int = 0
    alerts: int = 0
    partitions: list[str] = field(default_factory=list)


# --------------------------------------------------------------------------- puro


def make_run_id(now: datetime | None = None) -> str:
    """Identificador de corrida: fecha UTC compacta, valido como nombre de topic."""
    moment = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    return moment.strftime("%Y%m%dT%H%M%SZ")


def container_path(repo_root: Path, path: Path) -> str:
    """Traduce una ruta del host a la ruta que ve el contenedor de Spark.

    El contenedor monta el repo en ``/opt/rapidgo``; una ruta fuera del repo no existe
    para Spark, asi que se rechaza en vez de dejar que el job escriba en el vacio.
    """
    try:
        relative = Path(path).resolve().relative_to(Path(repo_root).resolve())
    except ValueError as exc:
        raise ValueError(
            f"{path} esta fuera del repositorio; el contenedor de Spark solo ve {repo_root}"
        ) from exc
    return f"{CONTAINER_REPO}/{relative.as_posix()}".rstrip("/")


def build_steps(config: RunConfig) -> list[Step]:
    """Comandos de la corrida en orden. La inyeccion de invalidos va entre 3 y 4."""
    py = config.python
    topic = config.topic
    lake = container_path(config.repo_root, config.lake_dir)
    checkpoints = container_path(config.repo_root, config.checkpoint_dir)

    def spark_step(name: str, job: str, checkpoint_name: str) -> Step:
        return Step(
            name,
            (
                py, "-m", "scripts.run_spark_job", job, "--",
                "--once",
                "--topic", topic,
                "--lake-dir", lake,
                "--checkpoint-dir", f"{checkpoints}/{checkpoint_name}",
            ),
        )

    return [
        Step("kafka", ("docker", "compose", "up", "-d", "--wait", "kafka")),
        Step(
            "topic",
            (py, "-m", "scripts.create_topic",
             "--topic", topic, "--bootstrap-servers", config.bootstrap_servers),
        ),
        Step(
            "producer",
            (
                py, "-m", "producer.rating_producer",
                "--topic", topic,
                "--bootstrap-servers", config.bootstrap_servers,
                "--rate", _num(config.rate),
                "--duration", _num(config.duration),
                "--seed", str(config.seed),
                "--degradation", _num(config.degradation),
                "--dominant-reason", config.dominant_reason,
            ),
        ),
        spark_step("ingesta", INGEST_JOB, "ingest"),
        spark_step("alertas", ALERTS_JOB, "alerts"),
    ]


def _num(value: float) -> str:
    """20.0 -> '20', 0.7 -> '0.7': argumentos limpios en el log."""
    return str(int(value)) if float(value).is_integer() else str(value)


def invalid_events(
    base: Mapping[str, Any], now: datetime | None = None
) -> list[tuple[str, dict[str, Any]]]:
    """Un evento invalido por regla de rechazo del contrato, con el motivo esperado.

    Los motivos son los de ``spark/transformations.py``; ``test_verify_pipeline``
    comprueba que cada evento produce exactamente el suyo.
    """
    now = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)

    def iso(moment: datetime) -> str:
        return moment.isoformat(timespec="milliseconds").replace("+00:00", "Z")

    def variant(index: int, **changes: Any) -> dict[str, Any]:
        event = dict(base)
        event["event_id"] = str(uuid.UUID(int=0xBAD0_0000 + index, version=4))
        event["order_id"] = f"ord-bad-{index:03d}"
        event["occurred_at"] = iso(now)
        event["rating"] = 2
        event["complaint_reason"] = "late_delivery"
        event.update(changes)
        return event

    missing = variant(1)
    missing.pop("customer_id", None)
    return [
        ("missing:customer_id", missing),
        ("rating_out_of_range", variant(2, rating=7, complaint_reason=None)),
        ("invalid_occurred_at", variant(3, occurred_at="ayer por la tarde")),
        ("future_occurred_at", variant(4, occurred_at=iso(now + timedelta(hours=2)))),
        ("invalid_channel", variant(5, channel="fax")),
        ("invalid_complaint_reason", variant(6, complaint_reason="mal_humor")),
        ("missing_complaint_reason", variant(7, complaint_reason=None)),
    ]


def parse_published(output: str) -> int:
    match = PUBLISHED_RE.search(output)
    if not match:
        raise VerificationError("el producer no imprimio la linea 'publicados=N'")
    return int(match.group(1))


def parse_lake(output: str) -> tuple[int, int, int]:
    """(raw, processed, rechazados) de la ultima linea ``LAKE ...`` del job de ingesta."""
    matches = LAKE_RE.findall(output)
    if not matches:
        raise VerificationError(
            "el job de ingesta no imprimio 'LAKE raw=.. processed=.. rechazados=..'"
        )
    raw, processed, rejected = matches[-1]
    return int(raw), int(processed), int(rejected)


def parse_alerts(output: str) -> int:
    matches = ALERTS_RE.findall(output)
    if not matches:
        raise VerificationError("el job de alertas no imprimio 'ALERTAS total=N'")
    return int(matches[-1])


def evaluate(observed: Observed) -> list[str]:
    """Reglas de aceptacion de #33. Lista vacia = la corrida cumple."""
    failures: list[str] = []
    if observed.published <= 0:
        failures.append("el producer no publico eventos (publicados=0)")
    if observed.raw != observed.processed:
        failures.append(
            f"raw={observed.raw} y processed={observed.processed} no coinciden: "
            "una de las dos capas perdio o duplico filas"
        )
    if observed.raw != observed.published:
        failures.append(
            f"raw={observed.raw} pero el producer publico {observed.published} validos: "
            "Spark no leyo todo el topic o la deduplicacion descarto eventos de mas"
        )
    if observed.rejected <= 0:
        failures.append(
            "rechazados=0 con datos invalidos inyectados: la ruta "
            "curated/ratings_rejected no recibio nada"
        )
    elif observed.rejected != observed.injected:
        failures.append(
            f"rechazados={observed.rejected} pero se inyectaron {observed.injected} "
            "eventos invalidos (uno por regla del contrato)"
        )
    if observed.alerts <= 0:
        failures.append(
            "curated/alerts esta vacio: con --degradation deberian sonar A1 y A2"
        )
    return failures


def lake_tree_failures(lake: Path) -> list[str]:
    """Comprueba en disco las capas y particiones del lake, sin Spark."""
    failures: list[str] = []
    expected = {
        "raw/dt=*/hour=*": "raw particionado por dt y hour",
        "processed/dt=*": "processed particionado por dt",
        "curated/ratings_rejected": "rechazados",
        "curated/alerts": "alertas",
    }
    for pattern, label in expected.items():
        folders = [p for p in lake.glob(pattern) if p.is_dir()]
        if not any(any(folder.rglob("*.parquet")) for folder in folders):
            failures.append(f"no hay Parquet en {lake.name}/{pattern} ({label})")
    return failures


def partitions(lake: Path) -> list[str]:
    """Particiones de raw como texto, para el resumen."""
    return sorted(
        p.relative_to(lake / "raw").as_posix()
        for p in (lake / "raw").glob("dt=*/hour=*")
        if p.is_dir()
    )


def declared_flags(source: str) -> set[str]:
    """Flags que un job declara con ``add_argument``, leidos del codigo sin importarlo.

    Importar los jobs exige pyspark; leer el AST no, y alcanza para saber si el job
    sigue aceptando lo que este script le pasa.
    """
    flags: set[str] = set()
    for node in ast.walk(ast.parse(source)):
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "add_argument"
        ):
            for arg in node.args:
                if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
                    if arg.value.startswith("--"):
                        flags.add(arg.value)
    return flags


def structure_failures(repo_root: Path) -> list[str]:
    """Todo lo que se puede validar sin Docker: archivos, flags, comandos y fixtures."""
    failures: list[str] = []

    for relative in REQUIRED_PATHS:
        if not (repo_root / relative).is_file():
            failures.append(f"falta {relative}")
    if failures:
        return failures

    for job in JOBS:
        source = (repo_root / job).read_text(encoding="utf-8-sig")
        missing = [flag for flag in REQUIRED_JOB_FLAGS if flag not in declared_flags(source)]
        if missing:
            failures.append(f"{job} no acepta {', '.join(missing)}")

    compose = (repo_root / "docker-compose.yml").read_text(encoding="utf-8-sig")
    if "container_name: rapidgo-kafka" not in compose:
        failures.append(
            "docker-compose.yml no define el contenedor rapidgo-kafka que usa run_spark_job"
        )

    config = RunConfig(repo_root=repo_root, run_id=make_run_id())
    try:
        steps = build_steps(config)
    except ValueError as exc:
        failures.append(f"no se pudo armar la corrida: {exc}")
        return failures
    for step in steps:
        if step.argv[:3] == (config.python, "-m", "scripts.run_spark_job"):
            module_job = step.argv[3]
            if not (repo_root / module_job).is_file():
                failures.append(f"el paso {step.name} apunta a {module_job}, que no existe")

    try:
        base = load_sample(repo_root)
    except (OSError, ValueError) as exc:
        failures.append(f"samples/rating_event.json no se puede leer: {exc}")
        return failures
    failures.extend(_rejection_rule_failures(base))
    return failures


def _rejection_rule_failures(base: Mapping[str, Any]) -> list[str]:
    """Cada evento inyectado debe caer exactamente en su regla del contrato."""
    from spark.transformations import validation_reasons  # noqa: PLC0415

    failures: list[str] = []
    base_reasons = validation_reasons(base)
    if base_reasons:
        failures.append(f"samples/rating_event.json no es valido: {base_reasons}")
    for expected, event in invalid_events(base):
        reasons = validation_reasons(event)
        if reasons != [expected]:
            failures.append(
                f"el evento invalido '{expected}' produce {reasons} segun el contrato"
            )
    return failures


def load_sample(repo_root: Path) -> dict[str, Any]:
    """El sample esta indentado en varias lineas: se lee como JSON completo."""
    return json.loads((repo_root / "samples/rating_event.json").read_text(encoding="utf-8-sig"))


def summary_lines(config: RunConfig, observed: Observed) -> list[str]:
    return [
        f"VERIFY run_id={config.run_id} topic={config.topic}",
        f"VERIFY lake={config.lake_dir.relative_to(config.repo_root).as_posix()}",
        f"VERIFY publicados={observed.published} inyectados_invalidos={observed.injected}",
        f"VERIFY raw={observed.raw} processed={observed.processed} "
        f"rechazados={observed.rejected} alertas={observed.alerts}",
        f"VERIFY particiones_raw={', '.join(observed.partitions) or '(ninguna)'}",
    ]


def step_failure(index: int, total: int, step: Step, detail: str) -> str:
    """Mensaje de fallo que nombra el paso, para no tener que leer todo el log."""
    return f"FALLO en el paso {index}/{total} '{step.name}': {detail}"


# --------------------------------------------------------------------- con efectos


def docker_available() -> bool:
    if shutil.which("docker") is None:
        return False
    result = subprocess.run(
        ["docker", "info", "--format", "{{.ServerVersion}}"],
        capture_output=True, text=True,
    )
    return result.returncode == 0


def run_step(step: Step, cwd: Path) -> tuple[int, str]:
    """Ejecuta el paso mostrando su salida en vivo y devolviendola para parsear."""
    print(f"\n=== {step.name}: {' '.join(step.argv)}", flush=True)
    process = subprocess.Popen(
        list(step.argv),
        cwd=cwd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
        env={**os.environ, "PYTHONUNBUFFERED": "1"},
    )
    lines: list[str] = []
    assert process.stdout is not None
    for line in process.stdout:
        print(f"  | {line}", end="", flush=True)
        lines.append(line)
    return process.wait(), "".join(lines)


def publish_invalid(config: RunConfig, events: Iterable[tuple[str, dict[str, Any]]]) -> int:
    """Publica los invalidos en el topic de la corrida, con la misma key que el producer."""
    from kafka import KafkaProducer  # noqa: PLC0415

    producer = KafkaProducer(
        bootstrap_servers=config.bootstrap_servers,
        key_serializer=lambda key: key.encode("utf-8"),
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        acks="all",
        client_id="rapidgo-verify",
    )
    sent = 0
    try:
        for reason, event in events:
            producer.send(config.topic, key=event["order_id"], value=event).get(timeout=15)
            print(f"  | invalido publicado: {reason}", flush=True)
            sent += 1
        producer.flush(timeout=15)
    finally:
        producer.close(timeout=15)
    return sent


def delete_topic(config: RunConfig) -> None:
    """Borra el topic de la corrida. Si falla no tumba la verificacion: solo avisa."""
    try:
        from kafka.admin import KafkaAdminClient  # noqa: PLC0415

        admin = KafkaAdminClient(bootstrap_servers=config.bootstrap_servers)
        try:
            admin.delete_topics([config.topic])
        finally:
            admin.close()
        print(f"topic {config.topic} eliminado", flush=True)
    except Exception as exc:  # noqa: BLE001
        print(f"AVISO: no se pudo borrar {config.topic}: {exc}", file=sys.stderr)


def prepare_run_dir(config: RunConfig) -> None:
    """Crea el lake temporal con permisos abiertos.

    En Linux el contenedor de Spark escribe con otro usuario (uid 185); sin esto no
    puede crear el Parquet en una carpeta que creo el host.
    """
    for folder in (config.run_dir, config.lake_dir, config.checkpoint_dir):
        folder.mkdir(parents=True, exist_ok=True)
        folder.chmod(0o777)


def run_full(config: RunConfig, keep_topic: bool = False) -> int:
    if not docker_available():
        print(
            "FALLO: Docker no esta disponible (docker info fallo). Levanta Docker "
            "Desktop, o valida solo la estructura con --structure-only.",
            file=sys.stderr,
        )
        return EXIT_STEP

    structure = structure_failures(config.repo_root)
    if structure:
        for message in structure:
            print(f"FALLO estructura: {message}", file=sys.stderr)
        return EXIT_STRUCTURE

    prepare_run_dir(config)
    steps = build_steps(config)
    total = len(steps) + 1  # + inyeccion de invalidos
    observed = Observed()
    outputs: dict[str, str] = {}
    topic_created = False

    try:
        index = 0
        for step in steps:
            index += 1
            code, output = run_step(step, config.repo_root)
            outputs[step.name] = output
            if code != 0:
                print(step_failure(index, total, step, f"exit code {code}"), file=sys.stderr)
                return EXIT_STEP
            if step.name == "topic":
                topic_created = True
            if step.name == "producer":
                observed.published = parse_published(output)
                index += 1
                injection = Step("invalidos", ("(en proceso)",))
                print(f"\n=== {injection.name}: un evento por regla de rechazo", flush=True)
                try:
                    observed.injected = publish_invalid(
                        config, invalid_events(load_sample(config.repo_root))
                    )
                except Exception as exc:  # noqa: BLE001
                    print(step_failure(index, total, injection, str(exc)), file=sys.stderr)
                    return EXIT_STEP

        observed.raw, observed.processed, observed.rejected = parse_lake(outputs["ingesta"])
        observed.alerts = parse_alerts(outputs["alertas"])
    except VerificationError as exc:
        print(f"FALLO: {exc}", file=sys.stderr)
        return EXIT_VERIFICATION
    finally:
        if topic_created and not keep_topic:
            delete_topic(config)

    observed.partitions = partitions(config.lake_dir)
    failures = evaluate(observed) + lake_tree_failures(config.lake_dir)
    lines = summary_lines(config, observed)
    lines.append("VERIFY resultado=" + ("FALLO" if failures else "OK"))
    lines.extend(f"FALLO: {message}" for message in failures)

    print()
    for line in lines:
        print(line, file=sys.stderr if line.startswith("FALLO") else sys.stdout)
    (config.run_dir / "resumen.txt").write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"evidencia guardada en {config.run_dir.relative_to(config.repo_root).as_posix()}/")
    return EXIT_VERIFICATION if failures else EXIT_OK


def run_structure_only(repo_root: Path) -> int:
    failures = structure_failures(repo_root)
    for message in failures:
        print(f"FALLO estructura: {message}", file=sys.stderr)
    if failures:
        return EXIT_STRUCTURE
    config = RunConfig(repo_root=repo_root, run_id=make_run_id())
    print(f"ESTRUCTURA ok: {len(REQUIRED_PATHS)} archivos, {len(JOBS)} jobs con "
          f"{', '.join(REQUIRED_JOB_FLAGS)}, {len(invalid_events(load_sample(repo_root)))} "
          "reglas de rechazo verificadas")
    for index, step in enumerate(build_steps(config), start=1):
        print(f"  paso {index} {step.name}: {' '.join(step.argv)}")
    return EXIT_OK


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Verifica el pipeline producer -> Kafka -> Spark -> lake (#33)."
    )
    parser.add_argument(
        "--structure-only",
        action="store_true",
        help="no usa Docker: valida archivos, flags de los jobs y reglas de rechazo (CI)",
    )
    parser.add_argument("--rate", type=float, default=20.0, help="eventos por segundo")
    parser.add_argument("--duration", type=float, default=15.0, help="segundos del producer")
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--degradation", type=float, default=0.7)
    parser.add_argument("--dominant-reason", default="late_delivery")
    parser.add_argument("--bootstrap-servers", default=DEFAULT_BOOTSTRAP)
    parser.add_argument(
        "--keep-topic", action="store_true", help="no borra el topic de la corrida al final"
    )
    return parser.parse_args(argv)


def validate_args(args: argparse.Namespace) -> list[str]:
    errors: list[str] = []
    if args.rate <= 0 or args.duration <= 0:
        errors.append("--rate y --duration deben ser mayores que 0")
    if not 0.0 < args.degradation <= 1.0:
        errors.append("--degradation debe estar en (0, 1]: sin degradacion no suenan alertas")
    return errors


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    errors = validate_args(args)
    if errors:
        for message in errors:
            print(f"ERROR: {message}", file=sys.stderr)
        return EXIT_STRUCTURE

    if args.structure_only:
        return run_structure_only(REPO_ROOT)

    config = RunConfig(
        repo_root=REPO_ROOT,
        run_id=make_run_id(),
        rate=args.rate,
        duration=args.duration,
        seed=args.seed,
        degradation=args.degradation,
        dominant_reason=args.dominant_reason,
        bootstrap_servers=args.bootstrap_servers,
    )
    return run_full(config, keep_topic=args.keep_topic)


if __name__ == "__main__":
    raise SystemExit(main())
