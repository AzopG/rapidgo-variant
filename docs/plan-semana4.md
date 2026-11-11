# Plan Semana 4 — `v1.0.0` (cierre del proyecto)

**Equipo 7 · RapidGo 360 · Variante 7**
**Objetivo:** dejar el pipeline estable y reproducible, cerrar la documentación, publicar el release final y demostrar el sistema end-to-end en video.

> **Estado:** la semana 3 cerró con `v0.3.0-orchestration` publicado sobre `main` en `4dc4c45`.
> Los cuatro issues de la semana 4 (#20, #21, #22, #23) están abiertos y sin empezar.

---

## 1. Requerimientos de la entrega

### 1.1 Individuales (uno por integrante)

| Requisito | Cómo se demuestra |
| --- | --- |
| Un PR sustancial desde la cuenta propia | PR abierto por el autor, no por otro |
| Una review a un PR ajeno | Review aprobada sobre el PR de otro integrante |
| Al menos un issue asignado, cerrado vía PR | `Closes #NN` en el cuerpo del PR |
| Un `workflow_dispatch` desde la cuenta propia | Corrida en la pestaña Actions con el nombre del autor |

### 1.2 Técnicos (de los issues #20 a #23)

| Requisito | Issue |
| --- | --- |
| Todos los pasos del CI pasan sobre `main` | #20 |
| Sin issues abiertos sin justificar | #20 |
| Pipeline reproducible siguiendo el README desde cero | #20 |
| README final: instalación, arquitectura, ejecución, KPIs y cómo correr el DAG | #21 |
| Resultados representativos y limitaciones conocidas en el README | #21 |
| Tabla de contribuciones con issues y PR principales por integrante | #21 |
| Enlace al video en el README | #21 |
| Tag `v1.0.0` sobre `main` con CI verde | #22 |
| Release notes con alcance, KPIs, alertas y enlaces a releases previos | #22 |
| Los tres integrantes con un `workflow_dispatch` visible | #22 |
| Video de máximo 10 minutos con los tres integrantes | #23 |
| Cada integrante explica una parte técnica que implementó o revisó | #23 |
| Demo funcional de Docker, Kafka, Spark, lake y Airflow | #23 |
| Enlace al video publicado en README y release | #23 |

### 1.3 Cierre

- Release `v1.0.0` sobre `main`, después de que el quality gate, el README y el video estén mergeados.
- Matriz de cierre con los cuatro requisitos individuales de los tres integrantes.
- Los cuatro issues de la semana cerrados vía PR.

---

## 2. Reparto

| Integrante | Issues | PR esperados |
| --- | --- | --- |
| **Ivan** (`AzopG`) | #20, #23 | Quality gate + instalación limpia · video final |
| **Luis** (`vannysys`) | #21 | README final y tabla de contribuciones |
| **Andrés** (`FelipeMaldonado10`) | #22 | Workflow de release, tag y notas de `v1.0.0` |

> El reparto respeta la asignación real de los issues (#20 y #23 son de Ivan, #21 de Luis, #22 de Andrés) y el historial de revisiones de las semanas 2 y 3.

### Ivan — #20 y #23

**#20 Quality gate final.** El CI de `main` ya está verde; el trabajo real es que el pipeline sea **reproducible desde cero** y que el gate lo demuestre, no que lo declare. Alcance propuesto:

- Job de CI que corra `python -m scripts.verify_pipeline` **completo** (Kafka real vía `docker compose`, no solo `--structure-only`) sobre un lake temporal. Si el runner no lo permite, el gate cae a estructura y se documenta por qué (§5.2).
- `scripts/bootstrap_check.py` (o equivalente): verifica en limpio que existen venv, dependencias, topic, stack y estructura del lake, con un mensaje accionable por cada faltante.
- Barrido de issues abiertos: los cuatro de la semana 4 se cierran vía PR; no queda ninguno sin justificar.
- Evidencia: corrida final de Actions sobre `main` y salida de la prueba de instalación limpia.

**#23 Video final.** Coordina la grabación de los tres. Guion de 10 minutos máx.: problema y arquitectura (Ivan), streaming y lake (Andrés), orquestación y calidad (Luis), demo en vivo del DAG. Sin PR de código: se evidencia con el enlace publicado en README y release.

### Luis — #21

**README final.** Es documentación, pero el criterio pide datos reales del historial, no una tabla escrita a mano. Alcance propuesto:

- `scripts/contributions.py`: consulta `gh` (issues y PRs mergeados) y genera la tabla de contribuciones por integrante con issue, PR y review. Con test propio y salida determinista.
- Secciones del README: instalación desde cero, arquitectura, ejecución, KPIs y cómo correr el DAG (ya existen; hay que revisarlas contra el estado real), resultados representativos de las corridas de la semana 3, limitaciones conocidas.
- Corregir datos vencidos: la línea "23 Issues abiertos" ya no es cierta (hoy quedan 4).
- Enlace al video cuando exista (`#23`).
- Evidencia: README final en `main`.

**Dependencia:** necesita el enlace del video de #23. Si el video se retrasa, el README se mergea con la sección marcada `_pendiente_` y se actualiza en un commit posterior, sin bloquear el release.

### Andrés — #22

**Release `v1.0.0`.** El release no se crea a mano: se crea con un workflow, para que quede reproducible y para que el tag no dependa de una máquina local. Alcance propuesto:

- `.github/workflows/release.yml`: `workflow_dispatch` con un input de versión; valida que `main` esté verde, crea el tag anotado y publica el release con las notas.
- Alta de `workflow_dispatch` en el CI existente para que el requisito individual se pueda cumplir desde la cuenta de cada uno (ya existe la corrida manual del CI; falta la de release).
- Release notes: alcance, KPIs, alertas, enlaces a `v0.1.0-foundation`, `v0.2.0-streaming` y `v0.3.0-orchestration`.
- Evidencia: URL del release y las tres corridas manuales.

**Dependencia:** va último. Necesita el quality gate (#20) y el README (#21) en `main`.

---

## 3. Reviews y reglas de la semana

```
Andrés  revisa a  Ivan     (#20, #23)
Ivan    revisa a  Luis     (#21)
Luis    revisa a  Andrés   (#22)
```

| Regla | Detalle |
| --- | --- |
| Nadie mergea su propio PR | La protección de `main` exige una aprobación de otro integrante |
| Nada de push directo a `main` | Aunque un admin pueda |
| Un PR por bloque de trabajo | Chico y enfocado |
| Review el mismo día | Un PR que duerme 24 h corre la semana |
| `workflow_dispatch` una vez | Cada uno desde su cuenta, antes del cierre |
| `Closes #NN` en el cuerpo | No en un comentario: si no está en el cuerpo, el issue no se cierra solo |

**Ojo con `dismiss_stale_reviews`:** cada merge invalida las aprobaciones de los PRs abiertos. Se aprueba y mergea de a uno, y no se toca una rama ya aprobada.

---

## 4. Dependencias y orden

```
#20 (quality gate) ──► #23 (video) ──► #21 (README + video) ──► #22 (release v1.0.0)
                              │                                  ▲
                              └──────────────────────────────────┘
                                (el release enlaza el video)
```

1. **#20 primero.** El video y el release necesitan un pipeline que corra de punta a punta sin sorpresas.
2. **#23 en paralelo con la review de #20.** El video se graba sobre `main` estable; no espera al merge de #21.
3. **#21 cuando exista el enlace del video.** Si no existe, se mergea con la sección `_pendiente_` y se actualiza después.
4. **#22 al final.** Tag y release sobre `main` con todo mergeado y el CI verde.
5. **Cierre:** matriz de cierre + `workflow_dispatch` de los tres + verificación de que no quedan issues abiertos.

---

## 5. Decisiones que bloquean el arranque

### 5.1 Dónde se publica el video

| Opción | A favor | En contra |
| --- | --- | --- |
| **A. YouTube (no listado)** | Enlace estable, se puede embeber, no requiere cuenta corporativa | Requiere subir desde una cuenta del equipo |
| B. Google Drive | Sin fricción si ya tienen cuenta | El enlace cambia de permisos y a veces no abre para el evaluador |
| C. En el repo (LFS o externo) | Todo en un lugar | Archivos de video pesan; `main` no es para binarios |

**Recomendado:** A. El enlace va en el README y en las release notes; el video se graba sobre `main` con el DAG corriendo en vivo.

### 5.2 El quality gate end-to-end en CI

`verify_pipeline` completo necesita Docker con Kafka y Spark. Los runners de GitHub lo permiten, pero el costo en minutos y la fragilidad son reales.

| Opción | A favor | En contra |
| --- | --- | --- |
| **A. Job end-to-end con `docker compose`** | Es el gate que el issue pide: el pipeline es reproducible de verdad | Suma minutos y puede ser frágil por el arranque de Kafka |
| B. `--structure-only` y evidencia manual | CI rápido y estable | No demuestra que el pipeline corra; la reproducibilidad queda como promesa |

**Recomendado:** A, con `--structure-only` como fallback documentado y un timeout explícito. Si el job end-to-end no es estable en tres corridas, se degrada a B y se deja el motivo escrito en el README.

**Evidencia medida en Semana 4:** el mismo commit `7e60354` ejecutó el pipeline completo en Actions mediante el run [`43106882082`](https://github.com/AzopG/rapidgo-variant/actions/runs/43106882082). Los jobs `validar`, `dag` y `Quality Gate end-to-end` terminaron en `success`; el quality gate duró `1m57s` y produjo `VERIFY publicados=300 inyectados_invalidos=7`, `raw=300 processed=300 rechazados=7 alertas=12`, una partición `dt=2026-11-11/hour=21` y `VERIFY resultado=OK`. El runner limpio instaló las dependencias, levantó Kafka y ejecutó Spark contra el lake temporal.

### 5.3 Cómo se crea el tag

| Opción | A favor | En contra |
| --- | --- | --- |
| **A. Workflow `release.yml`** | Reproducible, deja la corrida como evidencia y cumple el `workflow_dispatch` | Más código que mantener |
| B. Tag y release a mano | Rápido | No deja evidencia de proceso y depende de la máquina de quien lo haga |

**Recomendado:** A, con el tag anotado creado por el workflow y las notas versionadas en `docs/releases/v1.0.0.md`.

---

## 6. Riesgos heredados y limpieza

| # | Qué | Dónde | Por qué importa |
| --- | --- | --- | --- |
| 1 | El README dice "23 Issues abiertos" y ya no es cierto | `README.md:341` | Un README final con datos falsos resta credibilidad justo en la entrega |
| 2 | El README anuncia "release en Semana 4" en la estructura del repo y no existe workflow de release | `README.md:83` | Promesa incumplida si no se implementa en #22 |
| 3 | El Kanban quedó con los items de la semana 3 sin `Status` | GitHub Projects | La rúbrica mira el tablero; se corrige en la fase 0 |
| 4 | El tag del release de la semana 3 dice `v0.3.0-orchestration` y el título del release dice `v0.3.0-orchestración` | GitHub Releases | Cosmético; se documenta y no se renombra |
| 5 | La prueba `test_kpi_trend_parity.py` falla en Windows local por el worker de Spark | `tests/` | No es un fallo del código; el README debe decirlo para que la instalación limpia no parezca rota |
| 6 | El job end-to-end de CI aún no tenía evidencia medida al redactar este plan | `.github/workflows/ci.yml` | Ya está resuelto: la corrida `43106882082` demostró que funciona en `1m57s` y dejó los conteos reales arriba |

---

## 7. Enlaces

- Issues de la semana: [#20](https://github.com/AzopG/rapidgo-variant/issues/20) · [#21](https://github.com/AzopG/rapidgo-variant/issues/21) · [#22](https://github.com/AzopG/rapidgo-variant/issues/22) · [#23](https://github.com/AzopG/rapidgo-variant/issues/23)
- Checkpoint de la semana 3: [`semana3.md`](checkpoints/semana3.md)
- Plan de la semana 3: [`plan-semana3.md`](plan-semana3.md)
- Contrato del evento: [`event-contract.md`](event-contract.md)
- KPIs y umbrales: [`kpis-alerts.md`](kpis-alerts.md)
- Decisiones: [ADR-001](decisions/ADR-001-kafka-key-partitions.md) · [ADR-002](decisions/ADR-002-spark-execution-environment.md) · [ADR-003](decisions/ADR-003-airflow-spark-execution.md) · [ADR-004](decisions/ADR-004-ci-dag-validation.md)

---

## 8. Matriz de cierre — Semana 4 (`v1.0.0`)

_Se completa al cerrar la semana._

| Integrante | GitHub | PR propio (sustancial) | Review a PR ajeno | Issue cerrado vía PR | workflow_dispatch | Estado |
| --- | --- | --- | --- | --- | --- | --- |
| Ivan Palacio | [AzopG](https://github.com/AzopG) | pendiente | pendiente | pendiente | pendiente | Abierto |
| Andrés Maldonado | [FelipeMaldonado10](https://github.com/FelipeMaldonado10) | pendiente | pendiente | pendiente | pendiente | Abierto |
| Luis Jaraba | [vannysys](https://github.com/vannysys) | pendiente | pendiente | pendiente | pendiente | Abierto |

### Checklist de release

- [ ] `main` con el CI verde y sin issues abiertos sin justificar (#20)
- [ ] README final con resultados, limitaciones y tabla de contribuciones (#21)
- [ ] Enlace del video en README y release (#23)
- [ ] `workflow_dispatch` visible de los tres integrantes
- [ ] Tag `v1.0.0` anotado sobre `main`
- [ ] Release notes con enlaces a `v0.1.0-foundation`, `v0.2.0-streaming` y `v0.3.0-orchestration`
