"""Contrato del workflow de release (#22).

No se puede disparar un `workflow_dispatch` desde pytest, así que se fija el contrato que
el issue y el plan exigen: un input de versión, permisos de escritura, validación de `main`
verde en el commit exacto, tag anotado y release creado desde las notas versionadas.
"""

from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = REPO_ROOT / ".github" / "workflows" / "release.yml"
NOTES = REPO_ROOT / "docs" / "releases" / "v1.0.0.md"

PREVIOUS_RELEASES = ("v0.1.0-foundation", "v0.2.0-streaming", "v0.3.0-orchestration")


def load_workflow() -> dict:
    return yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))


def triggers(workflow: dict) -> dict:
    # YAML 1.1 lee la clave `on` como el booleano True.
    return workflow.get("on") or workflow.get(True)


def step(workflow: dict, name: str) -> dict:
    steps = workflow["jobs"]["release"]["steps"]
    for item in steps:
        if item.get("name") == name:
            return item
    raise AssertionError(f"falta el paso '{name}' en release.yml")


def test_workflow_dispara_solo_por_workflow_dispatch() -> None:
    assert list(triggers(load_workflow())) == ["workflow_dispatch"]


def test_input_de_version_y_dry_run() -> None:
    inputs = triggers(load_workflow())["workflow_dispatch"]["inputs"]

    assert inputs["version"]["required"] is True
    assert inputs["version"]["type"] == "string"
    assert inputs["dry_run"]["required"] is False
    assert inputs["dry_run"]["type"] == "boolean"
    assert inputs["dry_run"]["default"] is False


def test_permisos_para_crear_tag_y_release() -> None:
    permissions = load_workflow()["permissions"]

    assert permissions["contents"] == "write"
    # El paso de validacion consulta las corridas del CI por la API: ese es el
    # permiso documentado para Actions. Con el repo publico pasaria igual sin el,
    # pero no se depende de una regla implicita.
    assert permissions["actions"] == "read"


def test_el_release_corre_sobre_main_con_timeout() -> None:
    job = load_workflow()["jobs"]["release"]

    assert job["runs-on"] == "ubuntu-latest"
    assert job["timeout-minutes"] == 10
    checkout = job["steps"][0]
    assert checkout["with"]["ref"] == "main"
    assert checkout["with"]["fetch-depth"] == 0


def test_valida_version_notas_y_rama() -> None:
    run = step(load_workflow(), "Validar version y notas")["run"]

    assert 'refs/heads/main' in run
    assert r"^v[0-9]+\.[0-9]+\.[0-9]+$" in run
    assert "docs/releases/${VERSION}.md" in run


def test_valida_que_main_este_verde_en_el_commit_que_se_taggea() -> None:
    run = step(load_workflow(), "Validar que main este verde en este commit")["run"]

    assert "actions/workflows/ci.yml/runs" in run
    assert "run_sha" in run and "head_sha" in run
    assert "success" in run


def test_un_tag_existente_solo_se_acepta_si_apunta_al_mismo_commit() -> None:
    run = step(load_workflow(), "Validar el estado del tag")["run"]

    assert "refs/tags/${VERSION}^{commit}" in run
    assert "git ls-remote" in run
    assert "${HEAD_SHA}" in run
    assert "TAG_EXISTS=true" in run
    assert "TAG_EXISTS=false" in run


def test_crea_tag_anotado() -> None:
    step_ = step(load_workflow(), "Crear tag anotado")

    assert step_["if"] == "${{ !inputs.dry_run }}"
    assert "git tag -a" in step_["run"]
    assert 'git push origin "refs/tags/${VERSION}"' in step_["run"]


def test_reintento_no_recrea_el_tag() -> None:
    run = step(load_workflow(), "Crear tag anotado")["run"]

    assert 'TAG_EXISTS:-false' in run
    assert "no se recrea" in run


def test_publica_el_release_con_las_notas_versionadas() -> None:
    step_ = step(load_workflow(), "Publicar release")

    assert step_["if"] == "${{ !inputs.dry_run }}"
    assert "gh release create" in step_["run"]
    assert "--notes-file" in step_["run"]
    assert "--verify-tag" in step_["run"]


def test_dry_run_no_crea_nada() -> None:
    workflow = load_workflow()

    for name in ("Crear tag anotado", "Publicar release"):
        assert step(workflow, name)["if"] == "${{ !inputs.dry_run }}"


def test_notas_de_v1_0_0_cubren_el_issue() -> None:
    text = NOTES.read_text(encoding="utf-8")

    assert text.startswith("# v1.0.0")
    assert "## KPIs y alertas" in text
    assert "**A1**" in text and "**A2**" in text
    assert "## Enlaces" in text
    for release in PREVIOUS_RELEASES:
        assert release in text, f"faltan los enlaces a {release}"
