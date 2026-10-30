# Plan Semana 2 — `v0.2.0-streaming`

**Equipo 7 · RapidGo 360 · Variante 7**
**Cierre: 29 de octubre de 2026**

Esta semana el pipeline tiene que dejar de ser un diseño y **correr**: producer →
Kafka → Spark → Data Lake, con pruebas y CI activo en cada Pull Request.

---

## Reglas de la semana

| Regla | Detalle |
| --- | --- |
| Nadie mergea su propio PR | La protección de `main` exige 1 aprobación de otro integrante. |
| Cada PR sale de la cuenta del autor | La autoría la define quién abre el PR, no el email del commit. |
| Nada de push directo a `main` | Aunque un admin pueda, rompe la evidencia. |
| Un PR por bloque de trabajo | Chico y enfocado. Nada de "el PR gigante del viernes". |
| Review el mismo día | Si un PR duerme 24 h, la semana se cae. |
| `workflow_dispatch` una vez | Cada uno lo ejecuta desde **su** cuenta antes del cierre. |

**Rotación de reviews**

```
Andrés  revisa a  Ivan
Ivan    revisa a  Luis
Luis    revisa a  Andrés
```

**Tablero:** mover la tarjeta a `IN PROGRESS` al empezar, `REVIEW` al abrir el PR y
`DONE` al mergear.

---

## Estado final al 29 de octubre

Doce PR de la semana en `main`. Los seis primeros cerraron el alcance funcional el 28; el
resto entró el 29 con el CI, la verificación end-to-end y los dos arreglos que volvieron
reproducible el pipeline.

| Fecha | PR | Qué entró | Autor | Merge |
| --- | --- | --- | --- | --- |
| 28 | [#28](https://github.com/AzopG/rapidgo-variant/pull/28) | Topic idempotente + listeners KRaft | Ivan | `f3ddb25` |
| 28 | [#27](https://github.com/AzopG/rapidgo-variant/pull/27) | Marca `v0.1.0-foundation` como liberado | Ivan | `3370a2e` |
| 28 | [#29](https://github.com/AzopG/rapidgo-variant/pull/29) | Producer con key `order_id` | Ivan | `2ed94d7` |
| 28 | [#30](https://github.com/AzopG/rapidgo-variant/pull/30) | ADR-002: Spark en contenedor | Ivan | `151b700` |
| 28 | [#31](https://github.com/AzopG/rapidgo-variant/pull/31) | Ingesta a Parquet con ruta de rechazo | Andrés | `7949e35` |
| 28 | [#32](https://github.com/AzopG/rapidgo-variant/pull/32) | Alertas A1 y A2 con cooldown | Andrés | `dff107a` |
| 28 | [#34](https://github.com/AzopG/rapidgo-variant/pull/34) | Plan y checkpoint de la semana | Ivan | `f13dc10` |
| 29 | [#35](https://github.com/AzopG/rapidgo-variant/pull/35) | Workflow de CI (#13) | Luis | `2e7d356` |
| 29 | [#37](https://github.com/AzopG/rapidgo-variant/pull/37) | `verify_pipeline` end-to-end (#33) | Luis | `17e589d` |
| 29 | [#40](https://github.com/AzopG/rapidgo-variant/pull/40) | `--structure-only` en CI (#33) | Luis | `6372555` |
| 29 | [#41](https://github.com/AzopG/rapidgo-variant/pull/41) | Serializar escritura en `curated/alerts` | Andrés | `d202675` |
| 29 | [#39](https://github.com/AzopG/rapidgo-variant/pull/39) | Líder de particiones + retries del producer | Ivan | `141e054` |

Issues cerrados vía PR: **#6, #7, #8, #9, #10, #11, #13, #14, #33, #38**.

---

## Ivan

Issues **#6**, **#7**, **#14** — entregados.

| Issue | Qué es | PR |
| --- | --- | --- |
| #7 | Topic `rapidgo.ratings`: 3 particiones, retención 7 días, script idempotente | [#28](https://github.com/AzopG/rapidgo-variant/pull/28) |
| #6 | Producer con key `order_id`, CLI y modo degradación | [#29](https://github.com/AzopG/rapidgo-variant/pull/29) |
| #14 | ADR-002 (en #30) **y** checkpoint de la semana (`docs/checkpoints/semana2.md`) | [#30](https://github.com/AzopG/rapidgo-variant/pull/30) + docs |

**Orden de merge: #28 → #29 → #30.** Estaban apilados: cada uno salía del anterior.

Pendiente de Ivan al cierre: release `v0.2.0-streaming`. El CI ya existe y está en verde,
que era la condición: un release sin Actions no tenía evidencia que mostrar.

Colaboró además en dos defectos que no eran de su alcance, porque trababan la verificación
end-to-end: el PR #39 (líder de particiones + retries del producer) y la revisión de #31,
#32, #35, #37, #40 y #41.

---

## Andrés

Issues **#8**, **#9**, **#10**, **#11**, **#12**.

### PR 1 — `spark-ingest` (#8 + #9) ✅

- [x] Esquema explícito y parseo del JSON del topic.
- [x] Al menos 3 transformaciones (timestamp normalizado, clasificación del rating,
      enriquecimiento con zona).
- [x] Filas inválidas derivadas a `datalake/curated/ratings_rejected`, con el motivo.
- [x] Deduplicación por `event_id` con watermark.
- [x] Parquet en `datalake/raw/dt=.../hour=...` y `datalake/processed/dt=...`.
- [x] Checkpoint en `checkpoints/` (fuera del lake).
- [x] Prueba de reinicio: correr dos veces sin producir y verificar que el conteo no cambia.

### PR 2 — `spark-alerts` (#10 + #11) ✅

- [x] **A1** (10 min): `avg(rating) < 3.60` con `n ≥ 20` → warning; `< 3.20` → critical;
      cooldown de 15 min por zona.
- [x] **A2** (15 min): un motivo con `≥ 30%` y `n_reclamos ≥ 10` → warning; `≥ 50%` →
      critical; evaluado global y por `zone_id`.
- [x] Ambas persistidas en `datalake/curated/alerts` con valor observado y umbral.

### PR 3 — cierre de #12 ✅ (sin PR)

Los tests de **#12** se entregaron dentro de #31 y #32
(`tests/test_spark_transformations.py` y `tests/test_alert_rules.py`), pero el issue no
se cerró porque esos PRs no lo referenciaban. En vez de abrir un PR que solo agregara un
documento, la trazabilidad regla → test quedó en el propio issue, con los nombres exactos
de cada test y una sección de lo que esos tests **no** cubren (dedup, idempotencia al
reiniciar, y la divergencia del `occurred_at` sin zona horaria). Cerrado el 30/10.

### Cómo correrlo

```bash
docker compose up -d kafka
python -m scripts.create_topic
python -m scripts.run_spark_job spark/streaming_job.py
```

Para disparar las alertas con datos controlados:

```bash
python -m producer.rating_producer --rate 10 --duration 30 --degradation 0.7 --dominant-reason late_delivery
```

### Se considera terminado cuando

El job corre, el log muestra micro-batches, y con `--degradation 0.7` las dos alertas
aparecen con su valor y su umbral.

---

## Luis

Issues **#13** y **#33**. **El #13 va primero: es el que traba la semana.**

El #12 pasó a Andrés porque el trabajo ya estaba hecho. El bloque que tenía Luis se
reemplaza por #33, que es lo que a la semana le falta de verdad: una verificación
reproducible de que el pipeline corre.

### PR 1 — `ci-workflow` (#13) ✅ `2e7d356`

- [x] `.github/workflows/ci.yml` con los tres triggers: `push`, `pull_request` y
      `workflow_dispatch`.
- [x] `python -m compileall -q producer spark tests scripts`.
- [x] `pytest -v`.
- [x] `docker compose config --quiet`.
- [x] Al menos un test en el repo, porque `pytest` sin tests sale con **exit code 5**.
- [x] Evidencia: corridas en la pestaña Actions del PR.

### PR 2 — `verify-pipeline` (#33) ✅ `17e589d`

- [x] `scripts/verify_pipeline.py`: levanta Kafka, crea el topic, publica con el
      producer, corre los dos jobs `--once` sobre un lake temporal y verifica conteos.
- [x] Exit code distinto de 0 indicando el paso que falló; 0 con resumen.
- [x] Modo sin Docker que valide solo estructura y argumentos, para que corra en CI.
- [x] `tests/test_verify_pipeline.py` de la parte pura, sin Docker.
- [x] No toca `spark/` ni `producer/`.

### PR 3 — `--structure-only` en CI (#33) ✅ `6372555`

El script ya existía; lo que faltaba era que corriera en cada PR. Tres líneas en el
workflow: valida los archivos, los flags de los dos jobs y el `container_name` del compose
sin necesitar Docker en el runner. Encontró su propio valor el mismo día: siguió en verde
después de #41, que tocó `spark/alerts_job.py`.

### Se considera terminado cuando

`pytest -v` está en verde en local y en el PR, la corrida de Actions queda visible, y
`verify_pipeline.py` termina en 0 sobre un lake limpio.

---

## Dependencias

```
#13 (CI) ──┬──────────────────────────────┐
           │                              │
#8 ──► #9 ──► #10 ──► #11 ──► #12 ──┐     ├──► checkpoint + release
                                    ├─────┘
                    #33 (E2E) ──────┘
```

`#13` puede mergearse solo y desbloquea el `workflow_dispatch` de los tres. `#33` no
depende de nadie más que del entorno ya resuelto.

---

## Trampas ya medidas (no las descubran de nuevo)

Estas costaron horas de prueba el 27 de octubre. Están en el
[ADR-002](decisions/ADR-002-spark-execution-environment.md).

1. **Spark no corre en el host.** En Windows no puede escribir Parquet sin `winutils.exe`,
   y el conector de Kafka falla por la misma causa. Usar `python -m scripts.run_spark_job`.
2. **`samples/rating_event.json` está indentado en 18 líneas.** El reader de Spark es JSON
   por línea: sin `multiLine` devuelve **18 filas nulas y ningún error**.
3. **Ventanas en `append` no emiten nada.** Con watermark, una ventana solo se publica
   cuando el watermark pasa su fin; una corrida corta escribe cero filas sin fallar. Usar
   `outputMode("update")` + `foreachBatch`.
4. **Kafka necesita el listener `CONTROLLER` en `advertised.listeners`.** Si falta, el
   broker aborta al arrancar. Ya está corregido en el compose (#28).
5. **`n_reclamos` cuenta eventos con `complaint_reason != null`**, no todos los eventos.
6. **PowerShell 5.1 rompe `gh --jq`.** Si un comando de `gh` falla raro, pipear el JSON a
   `python -c`.
7. **`dismiss_stale_reviews` quema la aprobación en cada merge.** Con PRs apilados, cada
   merge a `main` invalida la aprobación del siguiente. Aprobar **en orden**, nunca antes
   de mergear el de abajo.

---

## Cierre de la semana

| Requisito | Quién | Estado |
| --- | --- | --- |
| Un PR sustancial por integrante | los tres | Ivan ✅ (#28, #29, #30, #34, #39) · Andrés ✅ (#31, #32, #41) · Luis ✅ (#35, #37, #40) |
| Review a un PR ajeno | los tres | Ivan ✅ (#31, #32, #35, #37, #40, #41) · Andrés ✅ (#28–#30, #34, #39) · Luis ✅ (#42) |
| Issue asignado cerrado vía PR | los tres | Ivan ✅ (#6, #7, #14) · Andrés ✅ (#8–#11, y #12 en el issue) · Luis ✅ (#13, #33) |
| `workflow_dispatch` manual | los tres | ✅ los tres, corridas del 29 en verde |
| `docs/checkpoints/semana2.md` | Ivan | ✅ corregido con la evidencia final |
| Release `v0.2.0-streaming` | Ivan | ✅ publicado sobre `main` en `b1b0247` |

Los seis requisitos están cumplidos por los tres integrantes. El único que estuvo en riesgo
fue la review de Luis: la rotación planificada era `Luis revisa a Andrés` y se resolvió con
su review del PR #42, que además era el último de documentación de la semana.
