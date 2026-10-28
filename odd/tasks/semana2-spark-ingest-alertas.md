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
| T1 | `spark/transformations.py`: esquema, reglas de rechazo, 3 transformaciones, dedup | S1-S11 | inline | — |
| T2 | `spark/streaming_job.py`: Kafka → lake, checkpoints, foreachBatch, duration | S5, S9-14 | inline | — |
| T3 | `tests/test_spark_transformations.py` + corrida en contenedor con eventos inválidos inyectados | S5-S11 | inline | — |
| T4 | `spark/alert_rules.py`: semáforo A1/A2 + cooldown (puro) | S15-S19, S22, S23 | inline | — |
| T5 | `spark/alerts_job.py`: ventanas por `occurred_at`, global + zona, cooldown, `curated/alerts` | S16, S17, S19-S21 | inline | — |
| T6 | `tests/test_alert_rules.py` + corrida con `--degradation 0.7` | S18, S22 | inline | — |

## Log

- L1. Pedido del usuario, textual: "tenemos que empezar con mi parte del trabajo ivan el admin del repositorio nos paso esta guia para llevar el paso a paso revisa ya tienes credenciales de mi github" (con `plan-semana2.md` adjunto). Luego: "primero los rewiew".
- L2. Review de #28/#29/#30 publicado y aprobado con el broker real levantado. Hallazgo real: `scripts/create_topic.py:214` imprime `TOPIC` en vez del topic procesado. `order_id` del producer no es único (600 eventos → `ord-508376` repetido).
- L3. Entorno: Docker Desktop levantado; `.venv` con `pytest==8.3.3` + `kafka-python-ng==2.2.3`; 33 tests verdes antes de empezar.
- L4. Decisión de diseño: las reglas de negocio se escriben una sola vez sobre constantes compartidas; el job arma expresiones Spark desde esas mismas constantes y las reglas escalares de alerta quedan como funciones puras, para que los bordes de #12 se puedan probar sin Spark.
