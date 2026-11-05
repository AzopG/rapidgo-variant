"""Tests del contexto y los comandos que usa el DAG (Issue #15)."""

from __future__ import annotations

import sys
from datetime import datetime, timezone

import pytest

from orchestration.pipeline import (
    ALERTS_JOB,
    KAFKA_CONNECTOR,
    KPI_JOB,
    RunContext,
    build_context,
    make_run_id,
    producer_command,
    run_command,
    spark_env,
    spark_job_command,
    topic_command,
)

FIXED = datetime(2026, 11, 4, 12, 30, tzinfo=timezone.utc)


def context(tmp_path) -> RunContext:
    return build_context(
        now=FIXED,
        lake_root=tmp_path / "lake",
        checkpoint_root=tmp_path / "checkpoints",
        bootstrap_servers="kafka:29092",
    )


def test_run_id_es_fecha_utc_compacta():
    assert make_run_id(FIXED) == "20261104T123000Z"


def test_contexto_deriva_topic_y_rutas_del_run_id(tmp_path):
    ctx = context(tmp_path)
    assert ctx.topic == "rapidgo.ratings.run.20261104T123000Z"
    assert ctx.lake_dir == str(tmp_path / "lake" / ctx.run_id)
    assert ctx.checkpoint_dir == str(tmp_path / "checkpoints" / ctx.run_id)


def test_contexto_sobrevive_el_viaje_por_xcom(tmp_path):
    original = context(tmp_path)
    restored = RunContext.from_xcom(original.as_xcom())
    assert restored == original


def test_contexto_respeta_el_entorno(monkeypatch, tmp_path):
    monkeypatch.setenv("RAPIDGO_BOOTSTRAP", "kafka-test:9092")
    monkeypatch.setenv("RAPIDGO_LAKE_ROOT", str(tmp_path / "otro-lake"))
    ctx = build_context(now=FIXED)
    assert ctx.bootstrap_servers == "kafka-test:9092"
    assert ctx.lake_dir.startswith(str(tmp_path / "otro-lake"))


def test_topic_command_lleva_topic_y_broker(tmp_path):
    command = topic_command(context(tmp_path), python_bin="python")
    assert command[:3] == ["python", "-m", "scripts.create_topic"]
    assert "--topic" in command and "rapidgo.ratings.run.20261104T123000Z" in command
    assert "--bootstrap-servers" in command and "kafka:29092" in command


def test_producer_command_pide_backfill(tmp_path):
    command = producer_command(context(tmp_path), backfill_minutes=120, python_bin="python")
    assert command[:3] == ["python", "-m", "producer.rating_producer"]
    index = command.index("--backfill-minutes")
    assert command[index + 1] == "120"


def test_spark_job_command_apunta_al_lake_de_la_corrida(tmp_path):
    ctx = context(tmp_path)
    command = spark_job_command(ctx, ALERTS_JOB, ["--once"], python_bin="python")
    assert command[1].endswith(ALERTS_JOB.replace("/", "\\")) or command[1].endswith(ALERTS_JOB)
    assert command[command.index("--topic") + 1] == ctx.topic
    assert command[command.index("--lake-dir") + 1] == ctx.lake_dir
    assert command[command.index("--checkpoint-dir") + 1] == ctx.checkpoint_dir
    assert command[-1] == "--once"


def test_spark_job_command_sirve_para_los_tres_jobs(tmp_path):
    ctx = context(tmp_path)
    for job in ("spark/streaming_job.py", ALERTS_JOB):
        command = spark_job_command(ctx, job, python_bin="python")
        assert command[1].endswith(job.replace("/", "\\")) or command[1].endswith(job)
        assert "--checkpoint-dir" in command


def test_el_job_batch_de_kpis_no_recibe_flags_de_streaming(tmp_path):
    # kpis_job.py es batch: no lee Kafka ni guarda checkpoints, y argparse rechaza
    # los flags que no declara.
    command = spark_job_command(
        context(tmp_path), KPI_JOB, streaming=False, python_bin="python"
    )
    assert "--topic" not in command
    assert "--checkpoint-dir" not in command
    assert "--lake-dir" in command


def test_spark_env_configura_master_y_conector():
    env = spark_env(base={})
    assert "local[*]" in env["PYSPARK_SUBMIT_ARGS"]
    assert KAFKA_CONNECTOR in env["PYSPARK_SUBMIT_ARGS"]
    assert env["JAVA_HOME"].startswith("/usr/lib/jvm/java-17")


def test_spark_env_no_pisa_lo_que_ya_viene_del_contenedor():
    env = spark_env(base={"PYSPARK_SUBMIT_ARGS": "ya-configurado"})
    assert env["PYSPARK_SUBMIT_ARGS"] == "ya-configurado"


def test_run_command_falla_con_exit_code_distinto_de_cero():
    with pytest.raises(RuntimeError, match="exit code 3"):
        run_command([sys.executable, "-c", "raise SystemExit(3)"])


def test_run_command_devuelve_cero_cuando_el_comando_funciona():
    assert run_command([sys.executable, "-c", "pass"]) == 0
