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
| T1 | `docs/releases/v1.0.0.md`: alcance, KPIs, alertas, enlaces a los tres releases previos y verificación | S2, S6, S7 | inline | hecho (`1d656b6`), corregido por review |
| T2 | `.github/workflows/release.yml`: `workflow_dispatch` con `version` y `dry_run`, valida `main` verde en el SHA, tag anotado y release con las notas | S1, S5, S7 | inline | hecho (`1d656b6`), corregido por review |
| T3 | `tests/test_release_workflow.py`: contrato del workflow y de las notas | S1, S5, S6 | inline | hecho (`1d656b6`), 12 tests |
| T4 | Verificación local del workflow: `bash -n`, `dry_run` real y negativos (versión inválida, notas faltantes, tag existente, SHA ajeno) | S5 | inline | hecho (10/10) + paso 4 real con el `GITHUB_TOKEN` en Actions |
| T5 | PR con `Closes #22`, review de un compañero y merge | S8, S9 | delegado (review) | hecho: PR #52 aprobado por Ivan y mergeado por él como `f31e743` (9/9 PRs del proyecto sin auto-merge) |
| T6 | `workflow_dispatch` real del release y evidencia en #22 (URL del release y de las tres corridas manuales) | S1, S3, S4, S10 | inline | hecho: `dry_run` `43343282021`, release real `43343344957`, tag y release verificados |
| T7 | README: `v1.0.0` publicado y release actual; checklist de release del plan — commit inmediato después de publicar, como pidió Ivan en su review | S2 | inline | hecho en PR #53 (`296b509`) |

Risk: item 5 (release) — el workflow crea un tag público y publica un release; un error queda visible y es costoso de revertir.

## Log

- L1. Pedido del usuario, literal: "En el repo debo tener trabajo asignado hagamoslo por favor".
- L2. Estado al arrancar: `main` en `5b8733a`, PR #51 (README de #21) mergeado y #21 cerrado. Único issue abierto asignado a Andrés: #22. Ivan tiene #23 (video) abierto.
- L3. Se verificó que la evidencia de `workflow_dispatch` de los tres integrantes ya existe y es visible en Actions: Ivan `43333325295` y Luis `43335110563`, ambos sobre `main` y en verde; Andrés `43106882082` sobre la rama del quality gate y `42132179429` sobre `main`.
- L4. Review de Ivan en #52 (`CHANGES_REQUESTED`, 00:35 UTC): pidió `actions: read` en `permissions` porque, según la documentación, al declarar `permissions` lo no nombrado queda en `none` y el endpoint de corridas del CI devolvería 403; más dos menores (la tabla de notas decía "sobre `main`" pero `43106882082` corrió sobre la rama del quality gate; el README queda desactualizado al publicar).
  - Verificado: la regla de `permissions` es cierta (documentación de workflow syntax), pero el bloqueador **no reproduce**. Prueba con el token real: una corrida de `ci.yml` en una rama descartable ejecutó el paso "Validar que main este verde en este commit" extraído del `release.yml` del PR, con solo `contents: read` declarado, y salió `main verde en 5b8733aa…` con exit 0 (run `43340022586`, job del probe `completed/success`). Causa: el endpoint `/actions/workflows/{id}/runs` solo exige lectura del repositorio, y el repo es público.
  - Igual se agregó `actions: read`: es el permiso documentado para Actions y evita depender de la regla implícita del repo público. Costo cero, y el test lo fija.
  - Menor 1 aceptado: se cita `43338275082` de Andrés sobre `main` en `5b8733a` (más nueva que la `42132179429` que Ivan propuso).
  - Menor 2 aceptado: la actualización del README queda como T7, commit inmediato después de publicar el release.
  - Lección: probé los bloques con mi token de usuario, que tiene todos mis permisos, y por eso no vi la diferencia con el `GITHUB_TOKEN`. El hueco de verificación se cerró ejecutando el paso real dentro de Actions.
- L5. Publicación (2026-11-13, 01:10–01:15 UTC):
  - PR #52 aprobado por Ivan y mergeado por él como `f31e743`. Ivan autorizó que lo mergeara yo; no lo hice: la regla del plan (`:118`) es "nadie mergea su propio PR" y el repo la cumple 9 de 9 (#44–#52), así que se lo pedí a él.
  - CI del merge en verde (`43342841085`), requisito del workflow antes de taggear.
  - `dry_run=true` real: run `43343282021`, salida `main verde en f31e743…` y ningún tag creado (verificado contra `git/refs/tags`).
  - Release real: run `43343344957`, `completed/success`. Tag `v1.0.0` es un objeto `tag` (anotado), tagger `github-actions[bot]`, apunta a `f31e743` = `main`.
  - Cuerpo del release byte por byte igual a `docs/releases/v1.0.0.md` (6082 bytes), con los acentos y el em dash intactos. Se verificó comparando el blob de git contra el JSON de la API, no contra la consola: PowerShell decodifica el stdout de `gh` con la codificación de consola y mostraba `???` donde había multibyte. Falso positivo evitado.
  - #22 se cerró solo con el merge (`CLOSED`/`COMPLETED`, 01:10:16 UTC).
  - T7 en PR #53.
