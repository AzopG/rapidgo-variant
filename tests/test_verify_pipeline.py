"""Pruebas de la parte pura de scripts/verify_pipeline.py (sin Docker, sin Kafka)."""

from datetime import datetime, timezone
from pathlib import Path

import pytest

from scripts import verify_pipeline as vp
from spark.transformations import validation_reasons

REPO = Path(__file__).resolve().parents[1]
NOW = datetime(2026, 10, 28, 18, 30, 5, tzinfo=timezone.utc)

INGEST_OUTPUT = """\
JOB lake=/opt/rapidgo/.verify/x/lake checkpoints=... topic=t
BATCH 0 rechazados=7
BATCH 0 validos=300
LAKE raw=300 processed=300 rechazados=7
ARBOL raw: dt=2026-10-28/hour=18
"""
ALERTS_OUTPUT = """\
COOLDOWN claves_sembradas=0
A1 BATCH 0 evaluadas=6 alertas=6
ALERTAS total=9
ALERTA A1 critical zona=GLOBAL n=1 max_valor=2.1000
"""


def config(**overrides):
    values = {"repo_root": REPO, "run_id": vp.make_run_id(NOW), "python": "python"}
    values.update(overrides)
    return vp.RunConfig(**values)


def healthy(**overrides):
    values = dict(published=300, injected=7, raw=300, processed=300, rejected=7, alerts=9)
    values.update(overrides)
    return vp.Observed(**values)


# ---------------------------------------------------------------- armado de la corrida


def test_run_id_is_utc_and_valid_as_topic_name():
    run_id = vp.make_run_id(NOW)
    assert run_id == "20261028T183005Z"
    assert config().topic == "rapidgo.ratings.verify.20261028T183005Z"


def test_lake_lives_inside_the_repo_under_verify_dir():
    cfg = config()
    assert cfg.lake_dir == REPO / ".verify" / cfg.run_id / "lake"
    assert cfg.checkpoint_dir.parent == cfg.run_dir


def test_container_path_maps_repo_to_mount_point():
    assert vp.container_path(REPO, REPO / ".verify" / "r1" / "lake") == "/opt/rapidgo/.verify/r1/lake"


def test_container_path_rejects_paths_outside_the_repo(tmp_path):
    with pytest.raises(ValueError, match="fuera del repositorio"):
        vp.container_path(REPO, tmp_path / "lake")


def test_steps_run_in_pipeline_order():
    names = [step.name for step in vp.build_steps(config())]
    assert names == ["kafka", "topic", "producer", "ingesta", "alertas"]


def test_kafka_step_waits_for_healthcheck():
    kafka = vp.build_steps(config())[0]
    assert kafka.argv == ("docker", "compose", "up", "-d", "--wait", "kafka")


def test_every_step_uses_the_run_topic_not_the_main_one():
    cfg = config()
    for step in vp.build_steps(cfg)[1:]:
        assert step.argv[step.argv.index("--topic") + 1] == cfg.topic


def test_producer_step_passes_degradation_and_seed():
    producer = vp.build_steps(config(degradation=0.7, seed=7, rate=20.0))[2]
    argv = list(producer.argv)
    assert argv[argv.index("--degradation") + 1] == "0.7"
    assert argv[argv.index("--seed") + 1] == "7"
    assert argv[argv.index("--rate") + 1] == "20"


@pytest.mark.parametrize("index, job", [(3, vp.INGEST_JOB), (4, vp.ALERTS_JOB)])
def test_spark_steps_run_once_on_the_temporary_lake(index, job):
    cfg = config()
    argv = list(vp.build_steps(cfg)[index].argv)
    assert argv[1:4] == ["-m", "scripts.run_spark_job", job]
    assert "--once" in argv
    lake = argv[argv.index("--lake-dir") + 1]
    assert lake == f"/opt/rapidgo/.verify/{cfg.run_id}/lake"


def test_spark_steps_use_separate_checkpoints():
    steps = vp.build_steps(config())
    ingest = steps[3].argv[steps[3].argv.index("--checkpoint-dir") + 1]
    alerts = steps[4].argv[steps[4].argv.index("--checkpoint-dir") + 1]
    assert ingest != alerts
    assert "/lake" not in ingest and "/lake" not in alerts


# ------------------------------------------------------------- eventos invalidos


def test_one_invalid_event_per_contract_rule():
    events = vp.invalid_events(vp.load_sample(REPO), NOW)
    assert len(events) == 7
    assert len({event["event_id"] for _, event in events}) == 7


@pytest.mark.parametrize(
    "expected, event",
    vp.invalid_events(vp.load_sample(REPO)),
    ids=lambda value: value if isinstance(value, str) else "",
)
def test_each_invalid_event_hits_exactly_its_rule(expected, event):
    assert validation_reasons(event) == [expected]


def test_sample_used_as_base_is_valid():
    assert validation_reasons(vp.load_sample(REPO)) == []


# ------------------------------------------------------------------- lectura de salida


def test_parse_lake_reads_the_three_counts():
    assert vp.parse_lake(INGEST_OUTPUT) == (300, 300, 7)


def test_parse_lake_uses_the_last_summary_line():
    assert vp.parse_lake(INGEST_OUTPUT + "LAKE raw=310 processed=310 rechazados=7\n")[0] == 310


def test_parse_alerts_and_published():
    assert vp.parse_alerts(ALERTS_OUTPUT) == 9
    assert vp.parse_published("publicados=300 particiones_usadas=3 duracion_s=15.0") == 300


@pytest.mark.parametrize(
    "parser, label",
    [(vp.parse_lake, "LAKE"), (vp.parse_alerts, "ALERTAS"), (vp.parse_published, "publicados")],
)
def test_missing_summary_line_is_a_clear_error(parser, label):
    with pytest.raises(vp.VerificationError, match=label):
        parser("Traceback (most recent call last): ...")


# -------------------------------------------------------------------- evaluacion


def test_healthy_run_has_no_failures():
    assert vp.evaluate(healthy()) == []


def test_raw_and_processed_must_match():
    failures = vp.evaluate(healthy(processed=299))
    assert any("raw=300 y processed=299" in message for message in failures)


def test_raw_must_match_what_the_producer_published():
    failures = vp.evaluate(healthy(raw=290, processed=290))
    assert any("publico 300" in message for message in failures)


def test_no_rejections_fails_with_degraded_data():
    failures = vp.evaluate(healthy(rejected=0))
    assert any("rechazados=0" in message for message in failures)


def test_rejections_must_match_injected_events():
    failures = vp.evaluate(healthy(rejected=6))
    assert any("se inyectaron 7" in message for message in failures)


def test_empty_alerts_fails():
    failures = vp.evaluate(healthy(alerts=0))
    assert any("curated/alerts" in message for message in failures)


def test_failure_message_names_the_step():
    step = vp.build_steps(config())[3]
    message = vp.step_failure(5, 6, step, "exit code 1")
    assert message == "FALLO en el paso 5/6 'ingesta': exit code 1"


def test_summary_reports_counts_and_relative_lake():
    lines = vp.summary_lines(config(), healthy(partitions=["dt=2026-10-28/hour=18"]))
    assert "VERIFY raw=300 processed=300 rechazados=7 alertas=9" in lines
    assert any(line.startswith("VERIFY lake=.verify/") for line in lines)


# ---------------------------------------------------------------- lake en disco


def _parquet(folder: Path) -> None:
    folder.mkdir(parents=True, exist_ok=True)
    (folder / "part-0000.snappy.parquet").write_bytes(b"PAR1")


def test_lake_tree_complete(tmp_path):
    _parquet(tmp_path / "raw" / "dt=2026-10-28" / "hour=18")
    _parquet(tmp_path / "processed" / "dt=2026-10-28")
    _parquet(tmp_path / "curated" / "ratings_rejected")
    _parquet(tmp_path / "curated" / "alerts")
    assert vp.lake_tree_failures(tmp_path) == []
    assert vp.partitions(tmp_path) == ["dt=2026-10-28/hour=18"]


def test_lake_tree_reports_missing_layers(tmp_path):
    _parquet(tmp_path / "raw" / "dt=2026-10-28")  # sin hour=
    failures = vp.lake_tree_failures(tmp_path)
    assert len(failures) == 4
    assert any("raw/dt=*/hour=*" in message for message in failures)


# ------------------------------------------------------------------ estructura


def test_declared_flags_reads_add_argument_calls():
    source = (
        "import argparse\n"
        "p = argparse.ArgumentParser()\n"
        "p.add_argument('--once', action='store_true')\n"
        "p.add_argument('--lake-dir', default='x')\n"
        "p.add_argument('job')\n"
    )
    assert vp.declared_flags(source) == {"--once", "--lake-dir"}


def test_real_repo_structure_is_valid():
    assert vp.structure_failures(REPO) == []


def test_structure_reports_missing_files(tmp_path):
    failures = vp.structure_failures(tmp_path)
    assert "falta docker-compose.yml" in failures
    assert len(failures) == len(vp.REQUIRED_PATHS)


def test_structure_only_mode_exits_zero(capsys):
    assert vp.main(["--structure-only"]) == vp.EXIT_OK
    assert "ESTRUCTURA ok" in capsys.readouterr().out


def test_full_run_without_docker_exits_with_step_code(monkeypatch, capsys):
    monkeypatch.setattr(vp, "docker_available", lambda: False)
    assert vp.main([]) == vp.EXIT_STEP
    assert "--structure-only" in capsys.readouterr().err


@pytest.mark.parametrize(
    "argv", [["--rate", "0"], ["--duration", "-1"], ["--degradation", "0"], ["--degradation", "1.5"]]
)
def test_invalid_arguments_exit_with_structure_code(argv, capsys):
    assert vp.main(argv) == vp.EXIT_STRUCTURE
    assert "ERROR" in capsys.readouterr().err


# --------------------------------------------- orquestacion con pasos simulados


@pytest.fixture
def fake_repo(tmp_path, monkeypatch):
    """Repo temporal y pasos simulados: prueba run_full sin Docker ni Kafka."""
    (tmp_path / "samples").mkdir()
    (tmp_path / "samples" / "rating_event.json").write_text(
        (REPO / "samples" / "rating_event.json").read_text(encoding="utf-8"), encoding="utf-8"
    )
    monkeypatch.setattr(vp, "docker_available", lambda: True)
    monkeypatch.setattr(vp, "structure_failures", lambda repo_root: [])
    monkeypatch.setattr(vp, "publish_invalid", lambda cfg, events: len(list(events)))
    monkeypatch.setattr(vp, "delete_topic", lambda cfg: None)
    return tmp_path


def fake_steps(lake: Path, outputs: dict, failing: str | None = None):
    def run_step(step, cwd):
        if step.name == "ingesta":
            _parquet(lake / "raw" / "dt=2026-10-28" / "hour=18")
            _parquet(lake / "processed" / "dt=2026-10-28")
            _parquet(lake / "curated" / "ratings_rejected")
        if step.name == "alertas":
            _parquet(lake / "curated" / "alerts")
        return (1 if step.name == failing else 0), outputs.get(step.name, "")

    return run_step


OUTPUTS = {
    "producer": "publicados=300 particiones_usadas=3",
    "ingesta": INGEST_OUTPUT,
    "alertas": ALERTS_OUTPUT,
}


def test_full_run_passes_and_writes_summary(fake_repo, monkeypatch, capsys):
    cfg = config(repo_root=fake_repo)
    monkeypatch.setattr(vp, "run_step", fake_steps(cfg.lake_dir, OUTPUTS))
    assert vp.run_full(cfg) == vp.EXIT_OK
    summary = (cfg.run_dir / "resumen.txt").read_text(encoding="utf-8")
    assert "VERIFY resultado=OK" in summary
    assert "rechazados=7" in summary


def test_full_run_stops_at_the_failing_step(fake_repo, monkeypatch, capsys):
    cfg = config(repo_root=fake_repo)
    monkeypatch.setattr(vp, "run_step", fake_steps(cfg.lake_dir, OUTPUTS, failing="ingesta"))
    assert vp.run_full(cfg) == vp.EXIT_STEP
    assert "FALLO en el paso 5/6 'ingesta': exit code 1" in capsys.readouterr().err


def test_full_run_fails_when_counts_do_not_match(fake_repo, monkeypatch, capsys):
    cfg = config(repo_root=fake_repo)
    outputs = {**OUTPUTS, "ingesta": "LAKE raw=300 processed=300 rechazados=0\n"}
    monkeypatch.setattr(vp, "run_step", fake_steps(cfg.lake_dir, outputs))
    assert vp.run_full(cfg) == vp.EXIT_VERIFICATION
    assert "rechazados=0" in capsys.readouterr().err
