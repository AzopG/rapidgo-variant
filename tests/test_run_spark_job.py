"""Pruebas del wrapper de Spark (sin Docker)."""

from pathlib import Path

from scripts.run_spark_job import (
    CONTAINER_REPO,
    IVY_DIR,
    KAFKA_CONNECTOR,
    SPARK_IMAGE,
    SPARK_SUBMIT,
    build_command,
)

REPO = Path("D:/repo")


def test_command_runs_the_official_image():
    command = build_command(REPO, "spark/streaming_job.py", "rapidgo-variant_default")
    assert command[:3] == ["docker", "run", "--rm"]
    assert SPARK_IMAGE in command
    assert SPARK_SUBMIT in command


def test_command_joins_the_broker_network():
    command = build_command(REPO, "spark/streaming_job.py", "rapidgo-variant_default")
    index = command.index("--network")
    assert command[index + 1] == "rapidgo-variant_default"


def test_command_mounts_repo():
    command = build_command(REPO, "spark/streaming_job.py", "net")
    mounts = [command[i + 1] for i, token in enumerate(command) if token == "-v"]
    assert mounts == [f"{REPO}:{CONTAINER_REPO}"]


def test_command_pins_connector_version_to_spark_version():
    command = build_command(REPO, "spark/streaming_job.py", "net")
    assert KAFKA_CONNECTOR.endswith("_2.13:4.0.1")
    assert SPARK_IMAGE.endswith(":4.0.1")
    assert command[command.index("--packages") + 1] == KAFKA_CONNECTOR


def test_command_sets_ivy_home_inside_container():
    command = build_command(REPO, "spark/streaming_job.py", "net")
    assert f"spark.jars.ivy={IVY_DIR}" in command


def test_job_path_is_inside_the_mounted_repo():
    command = build_command(REPO, "spark/streaming_job.py", "net")
    assert command[-1] == f"{CONTAINER_REPO}/spark/streaming_job.py"


def test_windows_separator_is_normalized():
    command = build_command(REPO, "spark\\jobs\\streaming.py", "net")
    assert command[-1] == f"{CONTAINER_REPO}/spark/jobs/streaming.py"


def test_job_arguments_are_appended():
    command = build_command(
        REPO, "spark/streaming_job.py", "net", ["--duration", "60"]
    )
    assert command[-2:] == ["--duration", "60"]
