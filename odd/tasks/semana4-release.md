# Semana 4 — Release `v1.0.0` (#22)

Objetivo: publicar el release final con un workflow reproducible que valide `main` verde en el commit que se taggea, cree el tag anotado y publique las notas versionadas del repositorio.
Rama: `feat/release-v1` sobre `main` en `5b8733a`. Un PR con `Closes #22`.
Runner: `python -m pytest` (CI, Python 3.13). El workflow se valida con `bash -n`, un `dry_run` real contra la API de GitHub y tres casos negativos.

## Specs

### #22 — texto del issue, literal

- S1. "Tag `v1.0.0` sobre `main` con CI verde."
- S2. "Release notes con alcance, KPIs, alertas y enlaces a los releases previos."
- S3. "Cada integrante ejecutó al menos un `workflow_dispatch` visible."
- S4. "Evidencia: URL del release y de las tres corridas manuales."

### Requisito del plan de la semana (`docs/plan-semana4.md:95-97`), literal

- S5. "`.github/workflows/release.yml`: `workflow_dispatch` con un input de versión; valida que `main` esté verde, crea el tag anotado y publica el release con las notas."
- S6. "Release notes: alcance, KPIs, alertas, enlaces a `v0.1.0-foundation`, `v0.2.0-streaming` y `v0.3.0-orchestration`."

### Decisión del plan (`docs/plan-semana4.md:178`), literal

- S7. "A, con el tag anotado creado por el workflow y las notas versionadas en `docs/releases/v1.0.0.md`."

### Requisitos de proceso del plan

- S8. `docs/plan-semana4.md:19`: "`Closes #NN` en el cuerpo del PR".
- S9. `docs/plan-semana4.md:118`: "Nadie mergea su propio PR".
- S10. `docs/plan-semana4.md:20`: "Un `workflow_dispatch` desde la cuenta propia".

### Condición de secuencia (`docs/plan-semana4.md:140-141`), literal

- S11. "Si el video se retrasa, el README se mergea con la sección marcada `_pendiente_` y se actualiza en un commit posterior, **sin bloquear el release**." → el video (#23) no bloquea #22.

Supuesto declarado (no es requisito del issue): las notas versionadas en `docs/releases/<tag>.md` son la fuente del cuerpo del release, para que el texto quede revisado en un PR y no escrito a mano en la interfaz de GitHub.

Alcance autorizado: `.github/workflows/release.yml`, `docs/releases/v1.0.0.md`, `tests/test_release_workflow.py`, `requirements.txt` (PyYAML para el test del workflow), `README.md` (roadmap de releases y enlace de release actual), `docs/plan-semana4.md` (checklist de release) y `odd/`. Nada de `spark/`, `producer/`, `airflow/`, `quality/`, `orchestration/` ni `docker-compose.yml`.

## Tasks

| ID | Tarea | Specs | Ruta | Estado |
| --- | --- | --- | --- | --- |
| T1 | `docs/releases/v1.0.0.md`: alcance, KPIs, alertas, enlaces a los tres releases previos y verificación | S2, S6, S7 | inline | pendiente |
| T2 | `.github/workflows/release.yml`: `workflow_dispatch` con `version` y `dry_run`, valida `main` verde en el SHA, tag anotado y release con las notas | S1, S5, S7 | inline | pendiente |
| T3 | `tests/test_release_workflow.py`: contrato del workflow y de las notas | S1, S5, S6 | inline | pendiente |
| T4 | Verificación local del workflow: `bash -n`, `dry_run` real y tres negativos (versión inválida, notas faltantes, tag existente) | S5 | inline | pendiente |
| T5 | PR con `Closes #22`, review de un compañero y merge | S8, S9 | delegado (review) | pendiente |
| T6 | `workflow_dispatch` real del release y evidencia en #22 (URL del release y de las tres corridas manuales) | S1, S3, S4, S10 | inline | pendiente |
| T7 | README: `v1.0.0` publicado y release actual; checklist de release del plan | S2 | inline | pendiente |

Risk: item 5 (release) — el workflow crea un tag público y publica un release; un error queda visible y es costoso de revertir.

## Log

- L1. Pedido del usuario, literal: "En el repo debo tener trabajo asignado hagamoslo por favor".
- L2. Estado al arrancar: `main` en `5b8733a`, PR #51 (README de #21) mergeado y #21 cerrado. Único issue abierto asignado a Andrés: #22. Ivan tiene #23 (video) abierto.
- L3. Se verificó que la evidencia de `workflow_dispatch` de los tres integrantes ya existe y es visible en Actions: Ivan `43333325295` y Luis `43335110563`, ambos sobre `main` y en verde; Andrés `43106882082` sobre la rama del quality gate y `42132179429` sobre `main`.
