# Checkpoint Semana 2 — RapidGo 360, Variante 7

**Equipo 7:** Andres Maldonado · Ivan Palacio · Luis Jaraba
**Release:** `v0.2.0-streaming` — tag sobre `main` posterior al merge de este documento (ver §7)
**Fecha de cierre:** 2026-10-29

## 1. Qué quedó funcionando

- Pipeline completo corriendo: producer → Kafka → Spark → Data Lake.
- Topic `rapidgo.ratings` con 3 particiones, retención de 7 días y creación idempotente.
  Kafka en modo KRaft, con el listener `CONTROLLER` y `ADVERTISED_LISTENERS` corregidos.
  El script de creación **no devuelve hasta que las particiones tienen líder**.
- Producer de `customer_rating` con key `order_id`, CLI, modo degradación controlada y
  reintentos para errores transitorios de Kafka.
- Ingesta en Structured Streaming con esquema explícito, tres transformaciones
  (timestamp normalizado a UTC, clasificación del rating, enriquecimiento con
  `zone_region`), derivación de filas inválidas a `curated/ratings_rejected` con el
  motivo, y deduplicación por `event_id` con watermark de 30 minutos.
- Lake por capas en Parquet: `datalake/raw/dt=/hour=` y `datalake/processed/dt=`, con los
  checkpoints en `checkpoints/`, fuera del lake.
- Alertas A1 (caída de calificación) y A2 (concentración de reclamos), evaluadas global y
  por `zone_id`, con cooldown de 15 minutos y persistencia serializada en
  `curated/alerts` con el valor observado, el umbral, la ventana y el tamaño de muestra.
- **157 tests** en verde (`pytest -q`), sin dependencia de Docker.
- CI en GitHub Actions con `compileall`, `pytest`, verificación estructural del pipeline
  y `docker compose config`, en `push`, `pull_request` y `workflow_dispatch`.
- Verificación end-to-end reproducible: `python -m scripts.verify_pipeline`.
- Spark corre en el contenedor `apache/spark:4.0.1` (ADR-002), no en el host.

## 2. Evidencia real observada

Sobre `main` en `141e054` (punta al cierre de este documento):

| Verificación | Resultado |
| --- | --- |
| `python -m pytest -q` | **157 passed** |
| `python -m compileall -q producer spark tests scripts` | exit 0 |
| `python -m scripts.verify_pipeline --structure-only` | `ESTRUCTURA ok: 11 archivos, 2 jobs con --once, --topic, --lake-dir, --checkpoint-dir, 7 reglas de rechazo verificadas` |
| CI en `main` (`d202675`, `6372555`, `141e054`) | **success** en los tres |

### Verificación end-to-end

Criterio de cierre de #38: `verify_pipeline` en 0 dos veces seguidas sobre `main` con los
dos arreglos de defectos ya mergeados. Dos corridas consecutivas:

```text
VERIFY run_id=20261029T190849Z topic=rapidgo.ratings.verify.20261029T190849Z
VERIFY publicados=300 inyectados_invalidos=7
VERIFY raw=300 processed=300 rechazados=7 alertas=12
VERIFY particiones_raw=dt=2026-10-29/hour=19
VERIFY resultado=OK
```

```text
VERIFY run_id=20261029T191216Z topic=rapidgo.ratings.verify.20261029T191216Z
VERIFY publicados=300 inyectados_invalidos=7
VERIFY raw=300 processed=300 rechazados=7 alertas=12
VERIFY particiones_raw=dt=2026-10-29/hour=19
VERIFY resultado=OK
```

`EXIT=0` en las dos. Es la primera vez que la verificación completa el recorrido de punta
a punta sin fallar.

### Corrida de ingesta (#31)

100 eventos válidos y 6 que violan el contrato. Los seis motivos corresponden, uno a uno,
a las seis reglas de `docs/event-contract.md`:

```text
LAKE raw=100 processed=100 rechazados=6
CLASES bueno=76, critico=16, medio=8
MOTIVOS future_occurred_at=1, invalid_channel=1, invalid_occurred_at=1,
        missing:customer_id=1, missing:complaint_reason=1, rating_out_of_range=1
```

Reinicio con los mismos checkpoints y sin producir: `raw=100 processed=100 rechazados=6`,
sin duplicar.

### Corrida de alertas (#32)

En `dff107a` se registró `ALERTAS total=9` (A1 6, A2 3). Con los dos arreglos de §4 la
misma corrida produce **`alertas=12` de forma reproducible** en todas las corridas
posteriores. No medí `dff107a` con el arnés actual, así que no afirmo que la diferencia
sea únicamente filas perdidas por la escritura concurrente; queda como observación, y el
número que vale es el reproducible.

Reinicio con los mismos checkpoints: `COOLDOWN claves_sembradas` sembrado desde
`curated/alerts`, sin repetir lo que ya sonó.

### `workflow_dispatch` de los tres integrantes

| Integrante | Corrida | Resultado |
| --- | --- | --- |
| `AzopG` | 2026-10-29 03:33:13Z | success |
| `vannysys` | 2026-10-29 13:36:19Z | success |
| `FelipeMaldonado10` | 2026-10-29 18:10:24Z | success |

### Merges de la semana

| PR | Qué entró | Autor | Merge |
| --- | --- | --- | --- |
| [#28](https://github.com/AzopG/rapidgo-variant/pull/28) | Topic idempotente + listeners KRaft | Ivan | `f3ddb25` |
| [#27](https://github.com/AzopG/rapidgo-variant/pull/27) | Marca `v0.1.0-foundation` como liberado | Ivan | `3370a2e` |
| [#29](https://github.com/AzopG/rapidgo-variant/pull/29) | Producer con key `order_id` | Ivan | `2ed94d7` |
| [#30](https://github.com/AzopG/rapidgo-variant/pull/30) | ADR-002: Spark en contenedor | Ivan | `151b700` |
| [#31](https://github.com/AzopG/rapidgo-variant/pull/31) | Ingesta a Parquet con ruta de rechazo | Andrés | `7949e35` |
| [#32](https://github.com/AzopG/rapidgo-variant/pull/32) | Alertas A1 y A2 con cooldown | Andrés | `dff107a` |
| [#34](https://github.com/AzopG/rapidgo-variant/pull/34) | Plan y checkpoint de la semana | Ivan | `f13dc10` |
| [#35](https://github.com/AzopG/rapidgo-variant/pull/35) | Workflow de CI (#13) | Luis | `2e7d356` |
| [#37](https://github.com/AzopG/rapidgo-variant/pull/37) | `verify_pipeline` end-to-end (#33) | Luis | `17e589d` |
| [#40](https://github.com/AzopG/rapidgo-variant/pull/40) | `--structure-only` en CI (#33) | Luis | `6372555` |
| [#41](https://github.com/AzopG/rapidgo-variant/pull/41) | Serializar escritura en `curated/alerts` | Andrés | `d202675` |
| [#39](https://github.com/AzopG/rapidgo-variant/pull/39) | Líder de particiones + retries del producer | Ivan | `141e054` |

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

## 4. Problemas reales y cómo se resolvieron

### 4.1 Los PRs apilados chocaron con `dismiss_stale_reviews`

Los PRs #28 → #29 → #30 salían uno del otro, y la protección de `main` invalida cualquier
aprobación cuando el diff contra la base cambia. Resultado: cada merge a `main` ponía el
siguiente PR en `REVIEW_REQUIRED` otra vez. La aprobación de #29 se cayó cinco veces y la
de #30 cuatro. Se resolvió mergeando en orden y re-aprobando después de cada merge; nunca
se pusheó a una rama ya aprobada.

El mismo mecanismo dio una segunda vuelta al mergear #31: #32 pasó a `CONFLICTING`
aunque **el contenido no tenía ningún conflicto**. GitHub calculaba la fusión con una base
única y veía los archivos de #31 como agregados por los dos lados. Se reprodujo localmente
(`git merge origin/main --no-commit`, sin conflictos, árbol idéntico), se publicó el merge
commit `8e82db5` —que no cambia ni un byte— y GitHub volvió a verlo `MERGEABLE`.

Aprendizaje: con PRs apilados y `dismiss_stale_reviews`, la aprobación es un recurso que
se quema en cada merge. Se aprueba en orden, y no se toca una rama aprobada.

### 4.2 El pipeline no era reproducible (#38)

`verify_pipeline` fallaba de forma intermitente en dos pasos distintos. Los dos defectos
eran reales y hacían falta los dos arreglos.

**Bloqueante 1 — `NotLeaderForPartitionError` (#39).** Kafka publica el topic en el
metadata antes de elegir líder de sus particiones; publicar en esa ventana falla. Peor:
`KafkaProducer` no fijaba `retries` y el default de kafka-python es **0**, así que el
primer error retriable mataba la corrida entera. Antes del fix, el paso 3 fallaba **2 de
3 corridas**. Se agregó espera activa de líderes en `create_topic` y `retries=5` /
`retry_backoff_ms=500` en el producer.

Matiz medido, y es el importante: en todas las corridas posteriores la espera **nunca
tuvo que esperar** — los líderes ya estaban asignados en el primer sondeo. Quien sostiene
el verde son los **retries**; la espera endurece el contrato de `create_topic` (que no
devuelva hasta que el topic sea escribible). Lo dejamos escrito así en el PR para no
atribuirle al cambio equivocado el mérito.

**Bloqueante 2 — dos committers sobre `curated/alerts` (#41).** Las dos queries de alertas
hacen `append` sobre la misma ruta desde hilos distintos. Spark no admite dos committers
simultáneos: comparten `_temporary/0`, que es único **por ruta de salida, no por
partición**. El síntoma es `ParentNotDirectoryException` o `FileNotFoundException` sobre
un `part-*` recién escrito, y se pierden filas. Aparecía aproximadamente 1 de cada 3
corridas.

Se descartó `partitionBy("alert_id")`, que era la hipótesis inicial: no habría cambiado
nada, porque la colisión no está en la partición de salida. La solución es serializar la
escritura con un lock de proceso en `spark/alerts_writer.py`.

**Medición que separa el mérito de cada arreglo**, para no atribuirle a uno lo que hizo el
otro:

| Estado de `main` | `verify_pipeline` |
| --- | --- |
| Sin #39, sin #41 | falla 2 de 3 en el paso 3 |
| Solo #39 | 3 corridas seguidas completas, pero el paso 6 seguía en riesgo por diseño |
| Solo #41 | corrida 1 OK, corrida 2 falla en el paso 3 con `NotLeaderForPartitionError` |
| **#39 + #41** | **3 de 3 en el branch de review y 2 de 2 sobre `main`** |

## 5. Riesgos y limitaciones conocidas

Se entrega con estas limitaciones documentadas, no ocultas:

1. **Las ventanas de las alertas son fijas (tumbling), no deslizantes.**
   `docs/kpis-alerts.md` define K1 y K2 como "deslizante cada 1 min", pero el job usa
   `F.window("10 minutes")`. Consecuencia: una caída que empieza al inicio de un bucket se
   reporta hasta 10 minutos después, y un incidente en el borde de dos ventanas se parte en
   dos y puede no alcanzar el mínimo de muestra. Los issues #10 y #11 no exigían
   deslizamiento, así que no bloqueó la entrega. Decisión pendiente: documentar que son
   fijas a propósito, o pasar a `window(..., "10 minutes", "1 minute")`.
2. **El lock de `curated/alerts` es de proceso.** El job de alertas es un único proceso con
   dos queries dentro, así que alcanza. Si el job se parte en dos procesos escribiendo al
   mismo lake, la garantía desaparece.
3. **La deduplicación por `event_id` aplica solo a `raw`.** El stream de rechazados no
   tiene dedup: un evento inválido reenviado se rechaza dos veces.
4. **Los validadores puros de los tests son espejos del job, no el job.** Un
   `occurred_at` sin zona horaria se rechaza en el test pero `try_to_timestamp` lo acepta
   en producción. Los tests fijan la regla que queremos, no la que el job aplica.
5. **`rejection_reason` mezcla separadores `,` y `;`** en motivos compuestos, así que no se
   puede parsear sin normalizar.
6. **`RATING_CLASSES` quedó como código muerto** después de mover la clasificación a SQL.
7. **Las alertas registran `evaluated=` contando alertas, no evaluaciones**, así que esa
   métrica del log no sirve para medir carga.

Ninguna de estas afecta la corrección del pipeline para los criterios de la semana 2.

## 6. Pendiente al cierre

| Qué | Quién | Issue |
| --- | --- | --- |
| PR que cierre formalmente los tests de contrato y reglas | Andrés | #12 |

Todo lo demás del alcance de la semana está cerrado: #13 y #33 (CI y verificación
end-to-end), #38 (pipeline reproducible), y el `workflow_dispatch` de los tres integrantes.

## 7. Release `v0.2.0-streaming`

Se publica sobre `main` después del merge de este documento. Contenido verificado: los 157
tests, el CI en verde y las dos corridas consecutivas de `verify_pipeline` de §2.

Queda **por fuera** del release la creación del DAG de Airflow, las tareas de calidad sobre
`raw` y `curated`, y el quality gate: son alcance de la semana 3.

## 8. Enlaces

- Tablero: https://github.com/users/AzopG/projects/1
- Contrato: [`docs/event-contract.md`](../event-contract.md)
- KPIs y alertas: [`docs/kpis-alerts.md`](../kpis-alerts.md)
- ADR-002: [`docs/decisions/ADR-002-spark-execution-environment.md`](../decisions/ADR-002-spark-execution-environment.md)
- Plan de la semana: [`docs/plan-semana2.md`](../plan-semana2.md)
- Guía de operación del pipeline: [`README.md`](../../README.md)
