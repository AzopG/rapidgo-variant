# Checkpoint Semana 2 — RapidGo 360, Variante 7

**Equipo 7:** Andres Maldonado · Ivan Palacio · Luis Jaraba
**Release:** `v0.2.0-streaming` *(pendiente de publicar: espera el workflow de CI)*
**Fecha:** 2026-10-28

## 1. Qué quedó funcionando

- Pipeline completo corriendo: producer → Kafka → Spark → Data Lake.
- Topic `rapidgo.ratings` con 3 particiones, retención de 7 días y creación idempotente.
  Kafka en modo KRaft, con el listener `CONTROLLER` y `ADVERTISED_LISTENERS` corregidos.
- Producer de `customer_rating` con key `order_id`, CLI y modo degradación controlada.
- Ingesta en Structured Streaming con esquema explícito, tres transformaciones
  (timestamp normalizado a UTC, clasificación del rating, enriquecimiento con
  `zone_region`), derivación de filas inválidas a `curated/ratings_rejected` con el
  motivo, y deduplicación por `event_id` con watermark de 30 minutos.
- Lake por capas en Parquet: `datalake/raw/dt=/hour=` y `datalake/processed/dt=`, con los
  checkpoints en `checkpoints/`, fuera del lake.
- Alertas A1 (caída de calificación) y A2 (concentración de reclamos), evaluadas global y
  por `zone_id`, con cooldown de 15 minutos y persistencia en `curated/alerts` con el
  valor observado, el umbral, la ventana y el tamaño de muestra.
- **95 tests** en verde (`pytest -q`), sin dependencia de Docker.
- Spark corre en el contenedor `apache/spark:4.0.1` (ADR-002), no en el host.

## 2. Evidencia real observada

Verificaciones hechas sobre `dff107a` (punta de `main` al cierre de este documento):

| Verificación | Resultado |
| --- | --- |
| `python -m pytest -q` | **95 passed** |
| `python -m compileall -q producer spark tests scripts` | exit 0 |
| `docker compose config --quiet` | exit 0 |

Corrida de ingesta (#31), 100 eventos válidos + 6 que violan el contrato:

```text
BATCH 0 rechazados=6
BATCH 0 validos=100
LAKE raw=100 processed=100 rechazados=6
ARBOL raw: dt=2026-10-28/hour=16
CLASES bueno=76, critico=16, medio=8
MOTIVOS future_occurred_at=1, invalid_channel=1, invalid_occurred_at=1,
        missing:customer_id=1, missing:complaint_reason=1, rating_out_of_range=1
```

Los seis motivos corresponden, uno a uno, a las seis reglas de `docs/event-contract.md`.
Reinicio con los mismos checkpoints y sin producir: `raw=100 processed=100
rechazados=6`, sin duplicar.

Corrida de alertas (#32), 300 eventos con `--degradation 0.7 --dominant-reason
late_delivery`:

```text
A1 BATCH 0 evaluadas=6 alertas=6
A2 BATCH 0 evaluadas=6 alertas=3
ALERTAS total=9
```

Reinicio con los mismos checkpoints: `COOLDOWN claves_sembradas=9` y `ALERTAS total=9`.
El cooldown se siembra desde `curated/alerts`, así que un reinicio no repite lo que ya
sonó.

Merges de la semana:

| Hora (UTC) | PR | Merge | Quién mergeó |
| --- | --- | --- | --- |
| 16:12 | #28 topic | `f3ddb25` | Andrés |
| 16:38 | #27 doc `v0.1.0` | `3370a2e` | Andrés |
| 16:41 | #29 producer | `2ed94d7` | Andrés |
| 16:41 | #30 ADR-002 | `151b700` | Andrés |
| 16:56 | #31 ingesta | `7949e35` | Ivan |
| 17:02 | #32 alertas | `dff107a` | Ivan |

## 3. Decisión técnica más importante

**Spark corre en contenedor, no en el host** (ADR-002). En Windows, Spark no puede
escribir Parquet sin `winutils.exe` y el conector de Kafka falla por la misma causa; el
`apache/spark:4.0.1` con el repo montado en `/opt/rapidgo` es el único camino que
funcionó. `scripts/run_spark_job.py` es la única forma soportada de lanzar los jobs:
resuelve el `HOME` roto del contenedor y la configuración de ivy.

Dos decisiones derivadas que costaron tiempo:

- **`try_to_timestamp` en vez de `to_timestamp`.** Con ANSI activo —default en Spark 4—
  un `occurred_at` malformado lanza `CAST_INVALID_INPUT` y aborta el micro-batch entero.
  El contrato pide descartar el evento, no tirar el lote.
- **A2 sin join de streams.** En `outputMode("update")` Spark rechaza el join entre dos
  DataFrames de streaming, así que la participación se calcula con conteos condicionales
  por motivo (la enumeración del contrato es cerrada) en una sola agregación.

## 4. Problema real y cómo se resolvió

**Los PRs apilados chocaron con `dismiss_stale_reviews`.** Los PRs #28 → #29 → #30
salían uno del otro, y la protección de `main` invalida cualquier aprobación cuando el
diff contra la base cambia. Resultado: cada merge a `main` ponía el siguiente PR en
`REVIEW_REQUIRED` otra vez. La aprobación de #29 se cayó cinco veces y la de #30 cuatro.
Se resolvió mergeando en orden y re-aprobando después de cada merge; nunca se pusheó a
una rama ya aprobada.

El mismo mecanismo dio una segunda vuelta al mergear #31: #32 pasó a `CONFLICTING`
aunque **el contenido no tenía ningún conflicto**. GitHub calculaba la fusión con una
base única y veía los archivos de #31 como agregados por los dos lados. Se reprodujo
localmente (`git merge origin/main --no-commit`, sin conflictos, árbol idéntico), se
publicó el merge commit `8e82db5` —que no cambia ni un byte— y GitHub volvió a verlo
`MERGEABLE`.

Aprendizaje: con PRs apilados y `dismiss_stale_reviews`, la aprobación es un recurso que
se quema en cada merge. Se aprueba en orden, y no se toca una rama aprobada.

## 5. Riesgo principal

**Las ventanas de las alertas son fijas, no deslizantes.** `docs/kpis-alerts.md` define
K1 y K2 como "deslizante cada 1 min", pero el job usa `F.window("10 minutes")`, que es
*tumbling*. Consecuencia: una caída que empieza al inicio de un bucket se reporta hasta
10 minutos después, y un incidente en el borde de dos ventanas se parte en dos y puede no
alcanzar el mínimo de muestra. Los issues #10 y #11 no exigían deslizamiento, así que no
bloqueó la entrega, pero la decisión tiene que quedar escrita: o se documenta que las
alertas usan ventanas fijas a propósito, o se pasa a `window(..., "10 minutes", "1
minute")`.

Segundo riesgo, menor: las dos queries de alertas escriben `append` sobre la misma ruta
`curated/alerts` en paralelo. Funcionó en las corridas, pero no es un patrón soportado;
`partitionBy("alert_id")` lo resuelve.

Y el riesgo que sigue abierto: **no hay CI en el repositorio**. Todos los PRs de la
semana muestran `checks: (ninguno)` porque `.github/workflows` no existe todavía (#13).
La protección de `main` no exige checks, así que el merge pasó igual, pero ninguna de las
95 pruebas se ejecutó en GitHub.

## 6. Pendiente al cierre

| Qué | Quién | Issue |
| --- | --- | --- |
| Workflow de CI con `compileall`, `pytest` y `docker compose config` | Luis | #13 |
| Script de verificación end-to-end del pipeline | Luis | #33 |
| PR que cierre formalmente los tests de contrato y reglas | Andrés | #12 |
| `workflow_dispatch` desde la cuenta de cada integrante (requisito individual) | los tres | — |
| Release `v0.2.0-streaming` | Ivan | — |

Los documentos de la semana (`plan-semana2.md` y este checkpoint) entraron en el PR de
cierre de documentación, con el plan actualizado a la reasignación de #12 y al alta de
#33.

## 7. Enlaces

- Tablero: https://github.com/users/AzopG/projects/1
- Contrato: [`docs/event-contract.md`](../event-contract.md)
- KPIs y alertas: [`docs/kpis-alerts.md`](../kpis-alerts.md)
- ADR-002: [`docs/decisions/ADR-002-spark-execution-environment.md`](../decisions/ADR-002-spark-execution-environment.md)
- Plan de la semana: [`docs/plan-semana2.md`](../plan-semana2.md)
- PRs: [#28](https://github.com/AzopG/rapidgo-variant/pull/28) · [#29](https://github.com/AzopG/rapidgo-variant/pull/29) · [#30](https://github.com/AzopG/rapidgo-variant/pull/30) · [#31](https://github.com/AzopG/rapidgo-variant/pull/31) · [#32](https://github.com/AzopG/rapidgo-variant/pull/32)
