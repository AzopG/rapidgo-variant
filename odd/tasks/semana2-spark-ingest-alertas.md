# Semana 2 — Spark: ingesta, lake y alertas (Issues #8, #9, #10, #11)

Rama `feat/spark-ingest` (sobre la punta del stack de Ivan: #28 → #29 → #30, ya aprobado).
Estrategia de entrega: `ask-on-risk` — PR 1 `spark-ingest` (#8 + #9), PR 2 `spark-alerts` (#10 + #11).
Runner: `pytest -v` desde `.venv`. Las corridas de Spark van en contenedor (`scripts/run_spark_job.py`, ADR-002).

## Specs

### #8 — parseo, validación y 3 transformaciones

- S1. "Esquema explicito y parseo del JSON."
- S2. "Al menos 3 transformaciones (normalizacion de timestamp, clasificacion de rating, enriquecimiento con zona)."
- S3. "Filas invalidas derivadas a `curated/ratings_rejected` con el motivo."
- S4. "Deduplicacion por `event_id`."
- S5. Evidencia: "Log del job con micro-batches procesados y conteo de validos vs rechazados."

Reglas de rechazo, citadas de `docs/event-contract.md:43-50`:

- S6. "1. Falta cualquiera de los campos obligatorios."
- S7. "2. `rating` está fuera de `1..5`."
- S8. "3. `occurred_at` no es una fecha ISO-8601 válida o es futura en más de 5 minutos."
- S9. "4. `channel` o `complaint_reason` no pertenecen a la enumeración."
- S10. "5. `rating <= 3` y `complaint_reason` es `null`."
- S11. "6. `rating >= 4` y `complaint_reason` no es `null` (se normaliza a `null`, no se descarta)."

### #9 — persistencia por capas

- S12. "`datalake/raw/dt=.../hour=...` y `datalake/processed/dt=...` en Parquet."
- S13. "Checkpoint fuera del lake, en `checkpoints/`."
- S14. "Reinicio del job sin duplicar datos." Evidencia: "Arbol del lake y prueba de reinicio con conteo estable."

### #10 — alerta A1

- S15. "`avg(rating) < 3.60` con `n >= 20` en 10 min = warning; `< 3.20` = critical."
- S16. "Cooldown de 15 min por zona."
- S17. "Persistencia en `curated/alerts` con valor observado y umbral."
- S18. Bordes fijados en el plan de la semana: "`3.59` con `n=20` → warning, `3.19` → critical, `3.60` → sin alerta, `3.59` con `n=19` → sin alerta."

### #11 — alerta A2

- S19. "Un motivo con `>= 30%` de reclamos y `n_reclamos >= 10` en 15 min = warning; `>= 50%` = critical."
- S20. "Evaluacion global y agrupada por `zone_id`."
- S21. "Persistencia en `curated/alerts` con la zona y el motivo."
- S22. Bordes fijados en el plan: "`0.29` → sin alerta, `0.30` con `n_reclamos=10` → warning, `0.49` → warning, `0.50` → critical."
- S23. `docs/event-contract.md:151` (trampa 5): "**`n_reclamos` cuenta eventos con `complaint_reason != null`**, no todos los eventos."

## Tasks

| ID | Qué | Specs | Ruta | Estado |
| --- | --- | --- | --- | --- |
| T1 | `spark/transformations.py`: esquema, reglas de rechazo, 3 transformaciones, dedup | S1-S11 | inline | ✅ `233b941` |
| T2 | `spark/streaming_job.py`: Kafka → lake, checkpoints, foreachBatch, duration | S5, S9-14 | inline | ✅ `233b941` |
| T3 | `tests/test_spark_transformations.py` + corrida en contenedor con eventos inválidos inyectados | S5-S11 | inline | ✅ `233b941` (69 tests) |
| T4 | `spark/alert_rules.py`: semáforo A1/A2 + cooldown (puro) | S15-S19, S22, S23 | inline | ✅ commit de alertas |
| T5 | `spark/alerts_job.py`: ventanas por `occurred_at`, global + zona, cooldown, `curated/alerts` | S16, S17, S19-S21 | inline | ✅ commit de alertas |
| T6 | `tests/test_alert_rules.py` + corrida con `--degradation 0.7` | S18, S22 | inline | ✅ commit de alertas (95 tests) |

## Log

- L1. Pedido del usuario, textual: "tenemos que empezar con mi parte del trabajo ivan el admin del repositorio nos paso esta guia para llevar el paso a paso revisa ya tienes credenciales de mi github" (con `plan-semana2.md` adjunto). Luego: "primero los rewiew".
- L2. Review de #28/#29/#30 publicado y aprobado con el broker real levantado. Hallazgo real: `scripts/create_topic.py:214` imprime `TOPIC` en vez del topic procesado. `order_id` del producer no es único (600 eventos → `ord-508376` repetido).
- L3. Entorno: Docker Desktop levantado; `.venv` con `pytest==8.3.3` + `kafka-python-ng==2.2.3`; 33 tests verdes antes de empezar.
- L4. Decisión de diseño: las reglas de negocio se escriben una sola vez sobre constantes compartidas; el job arma expresiones Spark desde esas mismas constantes y las reglas escalares de alerta quedan como funciones puras, para que los bordes de #12 se puedan probar sin Spark.
- L5. Evidencia del ingest (#8/#9), contenedor `apache/spark:4.0.1` con 100 eventos válidos y 6 inválidos publicados a propósito:
  ```text
  BATCH 0 rechazados=6
  BATCH 0 validos=100
  LAKE raw=100 processed=100 rechazados=6
  ARBOL raw: dt=2026-10-28/hour=16
  CLASES bueno=76, critico=16, medio=8
  MOTIVOS future_occurred_at=1, invalid_channel=1, invalid_occurred_at=1, missing:customer_id=1, missing:complaint_reason=1, rating_out_of_range=1
  ```
  Segunda corrida con los mismos checkpoints: `LAKE raw=100 processed=100 rechazados=6`, sin duplicar (S14).
- L6. Trampa encontrada en ejecución: `to_timestamp` con ANSI activo (default de Spark 4) lanza `CAST_INVALID_INPUT` sobre `occurred_at` malformado y aborta el batch, en vez de dejar la fila como rechazada. Se cambió a `try_to_timestamp`. También `concat_ws` devolvía `""` cuando no faltaba ningún campo, lo que dejaba el motivo con un `;` adelante; se resolvió con `nullif`.
- L7. Trampa de las alertas (ADR-002 trampa 3, confirmada): en `append` mode el KPI queda en `evaluadas=0` porque las ventanas sin watermark final no se emiten. Con `outputMode("update")` la primera corrida evaluó 6 ventanas A1 y 6 A2.
- L8. A2 no puede ser el join de dos streams: `Join between two streaming DataFrames/Datasets is not supported in Update output mode`. Se calcula la participación con conteos condicionales por motivo sobre la enumeración cerrada del contrato, en una sola agregación.
- L9. Evidencia de alertas con `--degradation 0.7` (300 eventos), tras `--once`:
  ```text
  A1 BATCH 0 evaluadas=6 alertas=6
  A2 BATCH 0 evaluadas=6 alertas=6
  ALERTAS total=9
  ALERTA A1 critical zona=GLOBAL n=1 max_valor=2.6533
  ALERTA A1 critical zona=Z-CENTRO n=1 max_valor=2.6379
  ALERTA A1 critical zona=Z-ESTE n=1 max_valor=2.3000
  ALERTA A1 critical zona=Z-NORTE n=1 max_valor=2.7553
  ALERTA A1 critical zona=Z-OESTE n=1 max_valor=2.6136
  ALERTA A1 critical zona=Z-SUR n=1 max_valor=2.7656
  ALERTA A2 critical zona=GLOBAL n=1 max_valor=0.9206
  ALERTA A2 critical zona=Z-CENTRO n=1 max_valor=0.9302
  ALERTA A2 critical zona=Z-ESTE n=1 max_valor=0.9091
  ```
  Los motivos con 30-50% en las otras zonas son `warning` y quedaron fuera del resumen porque se agrupa por severidad; el cooldown de 15 min por `(alerta, zona, motivo)` los dejó en una sola fila cada uno. Reinicio: `COOLDOWN claves_sembradas=9` y `ALERTAS total=9`, sin alertas nuevas.
- L10. El refactor de `spark/sources.py` se verificó con un smoke del ingest sobre `/tmp/lake-smoke`: `raw=300 processed=300 rechazados=0`, `CLASES bueno=86, critico=160, medio=54`. El lake y los checkpoints reales no se tocaron.
- L11. Entrega en dos PRs, apilados sobre la punta de Ivan (`630ccc2`): PR 1 `feat/spark-ingest` → `main` con `233b941` (#8 + #9, 69 tests), PR 2 `feat/spark-alerts` → `main` con `eb130ef` (#10 + #11 + refactor `sources.py`, 95 tests). PRs [#31](https://github.com/AzopG/rapidgo-variant/pull/31) y [#32](https://github.com/AzopG/rapidgo-variant/pull/32), sin mergear: la autoría es de Andrés y el merge lo hace otro integrante.
- L12. Las aprobaciones a #29 y #30 se cayeron solas cuando mergeó #28: GitHub rebaseó las ramas y descartó los reviews. Los commits revisados (`3cc4178`, `630ccc2`) no cambiaron, así que se re-aprobaron. Queda pendiente el `workflow_dispatch` porque el workflow de CI (#13, de Luis) todavía no existe en el repo.
- L13. #27, #29 y #30 mergeados a `main` (`3370a2e`, `2ed94d7`, `151b700`). Antes de eso, `gh pr merge` fallaba con "At least 1 approving review is required by reviewers with write access" aunque la aprobación estuviera puesta: la causa no era el review, era que la rama del PR había quedado atrás de `main` y la protección exige rama al día. `gh pr update-branch` y después aprobar lo destrabó. PR #31 y #32 actualizados con `main`; 69 y 95 tests siguen verdes.
