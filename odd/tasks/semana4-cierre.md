# Semana 4 — Cierre del proyecto `v1.0.0` (Issues #20–#23)

Rama de planificación: `docs/plan-semana4` sobre `main` en `4dc4c45`.
Estrategia de entrega: `ask-on-risk` — un PR por issue; el release va último.
Runner: `pytest -q` desde `.venv`. El CI de `main` está verde al arrancar.

## Specs

### #20 — Quality gate final y estabilización

Texto del issue, literal:

- S1. "Todos los pasos del CI pasan sobre `main`."
- S2. "Sin Issues abiertos sin justificar."
- S3. "Pipeline reproducible siguiendo el README desde cero."
- S4. "Evidencia: corrida final de Actions y prueba de instalación limpia."

Requisito del plan de la semana (`docs/plan-semana4.md:26-31`), literal:

- S5. "Job de CI que corra `python -m scripts.verify_pipeline` completo (Kafka real vía `docker compose`, no solo `--structure-only`) sobre un lake temporal."
- S6. "`scripts/bootstrap_check.py` (o equivalente): verifica en limpio que existen venv, dependencias, topic, stack y estructura del lake, con un mensaje accionable por cada faltante."

### #21 — README final

Texto del issue, literal:

- S7. "Instalacion, arquitectura, ejecucion, KPIs y como correr el DAG."
- S8. "Resultados representativos y limitaciones conocidas."
- S9. "Tabla de contribuciones con Issues y PR principales por integrante."
- S10. "Enlace al video."
- S11. "Evidencia: README final en `main`."

Requisito del plan (`docs/plan-semana4.md:57-60`), literal:

- S12. "`scripts/contributions.py`: consulta `gh` (issues y PRs mergeados) y genera la tabla de contribuciones por integrante con issue, PR y review. Con test propio y salida determinista."
- S13. "Corregir datos vencidos: la línea '23 Issues abiertos' ya no es cierta (hoy quedan 4)."

### #22 — Release `v1.0.0`

Texto del issue, literal:

- S14. "Tag `v1.0.0` sobre `main` con CI verde."
- S15. "Release notes con alcance, KPIs, alertas y enlaces a los releases previos."
- S16. "Cada integrante ejecuto al menos un `workflow_dispatch` visible."
- S17. "Evidencia: URL del release y de las tres corridas manuales."

Requisito del plan (`docs/plan-semana4.md:76-80`), literal:

- S18. "`.github/workflows/release.yml`: `workflow_dispatch` con un input de versión; valida que `main` esté verde, crea el tag anotado y publica el release con las notas."
- S19. "Alta de `workflow_dispatch` en el CI existente para que el requisito individual se pueda cumplir desde la cuenta de cada uno."
- S20. "Release notes: alcance, KPIs, alertas, enlaces a `v0.1.0-foundation`, `v0.2.0-streaming` y `v0.3.0-orchestration`."

### #23 — Video final

Texto del issue, literal:

- S21. "Duracion maxima de 10 minutos."
- S22. "Cada integrante explica una parte tecnica que implemento o reviso."
- S23. "Demo funcional de Docker, Kafka, Spark, lake y Airflow."
- S24. "Enlace publicado en el README y en el release."
- S25. "Evidencia: Enlace al video."

### Cierre

- S26. Matriz de cierre con los cuatro requisitos individuales de los tres integrantes.
- S27. Los cuatro issues de la semana cerrados vía PR (`Closes #NN` en el cuerpo).

## Decisiones

- **D1 (video):** YouTube no listado. Enlace estable y embebible; el video se graba sobre `main` con el DAG corriendo en vivo.
- **D2 (quality gate):** job end-to-end con `docker compose`; si no es estable en tres corridas, se degrada a `--structure-only` y se documenta el motivo.
- **D3 (tag):** workflow `release.yml` con `workflow_dispatch` y tag anotado; notas versionadas en `docs/releases/v1.0.0.md`.

## Tasks

| ID | Qué | Specs | Ruta | Estado |
| --- | --- | --- | --- | --- |
| T1 | `docs/plan-semana4.md` con reparto, dependencias y decisiones | S26 | inline | ✅ |
| T2 | Kanban: items S3 a DONE y S4 a READY con semana y responsable | S2 | inline | ✅ |
| T3 | #20 (Ivan): job end-to-end del CI + `scripts/bootstrap_check.py` | S1–S6 | inline | ⏳ |
| T4 | #21 (Luis): `scripts/contributions.py` + README final | S7–S13 | inline | ⏳ |
| T5 | #22 (Andrés): `.github/workflows/release.yml` + `docs/releases/v1.0.0.md` | S14–S20 | inline | ⏳ |
| T6 | #23 (Ivan): video de 10 min con los tres integrantes | S21–S25 | inline | ⏳ |
| T7 | Cierre: matriz, `workflow_dispatch` de los tres y verificación de issues | S26–S27 | inline | ⏳ |

## Log

- L1. Pedido del usuario, literal: "vamos con la preparacion de la semana 4".
- L2. El reparto sale de la asignación real de los issues: #20 y #23 de Ivan (`AzopG`), #21 de Luis (`vannysys`), #22 de Andrés (`FelipeMaldonado10`).
- L3. Orden: #20 → #23 → #21 → #22. El README (#21) depende del enlace del video (#23); el release (#22) depende de que todo esté en `main`.
- L4. Riesgos heredados detectados: README con "23 Issues abiertos" vencido, "release en Semana 4" sin workflow, Kanban desactualizado y la prueba de paridad de K3 que falla solo en Windows local.
