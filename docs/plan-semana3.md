# Plan Semana 3 — `v0.3.0-orchestration`

**Equipo 7 · RapidGo 360 · Variante 7**
**Objetivo:** orquestar con Airflow la calidad y la agregación sobre el lake, y demostrar que el pipeline se recupera de un fallo real.

> **Estado:** fase 0 **mergeada** en `main` como `638319d` (PR #44). La semana 3 arranca sobre esa base.

---

## 1. Requerimientos de la entrega

### 1.1 Individuales (uno por integrante)

| Requisito | Cómo se demuestra |
| --- | --- |
| Un PR sustancial desde la cuenta propia | PR abierto por el autor, no por otro |
| Una review a un PR ajeno | Review aprobada sobre el PR de otro integrante |
| Al menos un issue asignado, cerrado vía PR | `Closes #NN` en el cuerpo del PR |
| Un `workflow_dispatch` desde la cuenta propia | Corrida en la pestaña Actions con el nombre del autor |

### 1.2 Técnicos (de los issues #15 a #19)

| Requisito | Issue |
| --- | --- |
| Airflow en `docker-compose.yml`: webserver, scheduler y postgres | #15 |
| DAG con ≥5 tasks, dependencias explícitas, `retries` y paso de datos por XCom | #15 |
| DAG importable sin errores | #15 |
| Calidad sobre `raw`: vacíos, esquema, duplicados | #16 |
| Calidad sobre `curated`: los 3 KPIs presentes y en rango | #16 |
| Reporte generado por el DAG con los 3 KPIs y las alertas disparadas | #16 |
| Fallo reproducible, log real y corrida posterior exitosa | #17 |
| Paso de CI que valide la importación del DAG + pruebas nuevas de reglas de negocio | #18 |
| ADR-003 y `docs/checkpoints/semana3.md` con la tarea que falló, el mensaje real del log y el commit que lo resolvió | #19 |

### 1.3 Cierre

- Release `v0.3.0-orchestration` sobre `main`, después de que el checkpoint y el ADR estén mergeados.
- Matriz de cierre con los cuatro requisitos individuales de los tres integrantes.

---

## 2. Reparto

| Integrante | Issues | PR esperados |
| --- | --- | --- |
| **Ivan** (`AzopG`) | #15, #19 | DAG + stack de Airflow · ADR-003 + checkpoint |
| **Luis** (`vannysys`) | #16 | Funciones de calidad y generador del reporte |
| **Andrés** (`FelipeMaldonado10`) | #18, #17 | CI valida el DAG y pruebas nuevas · fallo controlado y recuperación |

### Ivan — #15 y #19

**#15 DAG de Airflow.** Stack en `docker-compose.yml` (webserver, scheduler, postgres) y el DAG con al menos cinco tasks, dependencias explícitas, `retries` y paso de datos por XCom. El DAG importa sin error y el grafo se ve en la UI.

**#19 ADR-003 y checkpoint.** El ADR documenta la estrategia de orquestación **y** la de recuperación ante fallos: contexto, opciones, decisión y trade-offs. El checkpoint va con la tarea que falló, el mensaje real del log y el commit que lo resolvió — eso sale de #17, así que depende de Andrés.

**Contrato con Luis:** define y escribe en el issue la signatura exacta de las funciones de calidad que el DAG va a llamar, antes de que Luis empiece. Si el contrato cambia después, el costo lo paga el otro.

### Luis — #16

Funciones de calidad sobre `raw` (vacíos, esquema, duplicados) y sobre `curated` (los 3 KPIs presentes y en rango), más el generador del reporte con los KPIs y las alertas disparadas.

Python puro, sin Airflow: las funciones reciben rutas y devuelven un diccionario que el DAG pasa por XCom. Con tests propios, porque si dependen del DAG para probarse, la semana se traba.

**Evidencia:** archivo de reporte generado por el DAG y salida de los tests.

### Andrés — #18 y #17

**#18 CI.** Un paso que valide la importación del DAG antes del merge, y las pruebas nuevas de reglas de negocio corriendo en CI. Verde en el release.

**#17 Fallo controlado.** Kafka detenido, `raw` vacío o esquema inválido: el fallo tiene que ser reproducible, con el log real del error y las tareas afectadas, y una corrida posterior exitosa después de la corrección.

Va después de #18 y de que exista el DAG: no se puede hacer fallar algo que no existe.

---

## 3. Reviews y reglas de la semana

```
Andrés  revisa a  Ivan     (#15, #19)
Ivan    revisa a  Luis     (#16)
Luis    revisa a  Andrés   (#18, #17)
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
                    ┌──► #18 (CI valida el DAG) ──────┐
#15 (compose + DAG) ┤                                  ├──► #19 (ADR-003 + checkpoint) ──► release
                    └──► #17 (fallo + recuperación) ───┘
#16 (calidad) ──────────────► lo llama el DAG
```

1. **Decisión de ejecución de Airflow** (§6), antes de escribir el DAG.
2. **Contrato DAG ↔ calidad** escrito en el issue #16, antes de que Luis empiece.
3. #15 y #16 en paralelo.
4. #18 cuando exista un DAG que importar.
5. #17 al final: necesita DAG, calidad y CI andando.
6. #19 cierra la semana con la evidencia de #17.

---

## 5. Fase 0 — parche de riesgos heredados

Todo lo que sigue quedó abierto al cerrar la semana 2, está verificado sobre `main` en `b1b0247`, y se corrigió **antes** de empezar la semana 3. Un solo PR, con un test por arreglo.

| # | Qué | Dónde | Por qué importa |
| --- | --- | --- | --- |
| 1 | `state[key] = moment` se ejecuta **antes** de `_emit`. Si la escritura falla, el cooldown queda marcado y la alerta se pierde en el reintento | `spark/alerts_job.py:116` y `:163` | Es pérdida silenciosa de datos: el DAG va a reintentar tareas y cada reintento puede comerse alertas |
| 2 | Las ventanas son **tumbling** (`F.window(col, A1_WINDOW)`) pero `docs/kpis-alerts.md` las define deslizantes cada 1 min | `spark/alerts_job.py:215` y `:240` | El reporte de #16 va a publicar KPIs calculados con una semántica distinta a la documentada |
| 3 | La deduplicación por `event_id` se aplica a los válidos, no al stream de rechazados | `spark/streaming_job.py:82` | El chequeo de duplicados de #16 sobre `curated` va a dar falsos positivos |
| 4 | `rejection_reason` mezcla separadores: `,` para campos faltantes y `;` para violaciones de regla | `spark/transformations.py:207` y `:233` | No se puede parsear el motivo sin normalizar antes, y #16 tiene que validar motivos |
| 5 | `RATING_CLASSES` quedó declarado y sin uso tras mover la clasificación a SQL | `spark/transformations.py:44` | Código muerto que sugiere una fuente de verdad que ya no existe |
| 6 | El contador del log se llama `evaluadas=` pero incrementa solo cuando una alerta **dispara**, antes del cooldown | `spark/alerts_job.py:111` y `:135` (igual en A2) | La métrica de carga es falsa; el reporte de #16 no puede usarla |
| 7 | Divergencia entre el validador puro y el job: `parse_occurred_at` exige zona horaria, pero el job rechaza por `event_ts IS NULL` y `try_to_timestamp` acepta un timestamp sin zona | `spark/transformations.py:123` vs `spark/sources.py:59` | El test `test_occurred_at_without_timezone_is_rejected` pasa mientras producción acepta el evento. **Reproducir primero**, después decidir: alinear el job o cambiar el contrato |
| 8 | BOM al inicio del archivo | `spark/alerts_job.py`, `spark/streaming_job.py` | Rompe el parseo en algunas herramientas y ensucia los diffs |

**Criterio de terminado de la fase 0:** los 157 tests en verde más un test nuevo por arreglo, y dos corridas de `verify_pipeline` consecutivas en 0. Si algún arreglo cambia el comportamiento observable (el 2 y el 7 lo hacen), la evidencia tiene que ser una corrida nueva, no la de la semana 2.

**Resultado medido** en la rama `fix/riesgos-semana2`:

| Verificación | Resultado |
| --- | --- |
| `pytest -q` | **172 passed** (157 previos + 15 de regresión) |
| `compileall` | exit 0 |
| `verify_pipeline` (1.ª corrida) | `raw=300 processed=300 rechazados=7 alertas=12`, exit 0 |
| `verify_pipeline` (2.ª corrida) | `raw=300 processed=300 rechazados=7 alertas=12`, exit 0 |

Las ventanas deslizantes no cambian el total de alertas de la corrida de verificación; el evento sin zona horaria que se inyecta ahora se rechaza en el job, no solo en el test.

**Quién:** Ivan lo escribió, Andrés lo revisa — es el área de código que Andrés conoce. Va como un PR único para no fragmentar la revisión.

---

## 6. Decisiones que bloquean el arranque

### 6.1 Cómo ejecuta Airflow los jobs de Spark

Hoy `scripts/run_spark_job.py` hace `docker run` desde el host. Un contenedor de Airflow no puede hacer eso sin que se le monte el socket de Docker.

| Opción | A favor | En contra |
| --- | --- | --- |
| **A. Imagen propia de Airflow con Java + PySpark, `spark-submit --master local[*]` adentro** | Sin docker-in-docker, reproducible, mismo camino en CI | Imagen más pesada de construir y mantener |
| B. Montar el socket de Docker y seguir usando `run_spark_job.py` | Menos código nuevo | Le da al contenedor control del daemon; feo de defender |

**Decidido:** opción A. Los jobs corren con `python <job>.py` dentro del contenedor de Airflow, con `--master local[*]` y el conector resuelto por Ivy. El detalle completo, las consecuencias y la estrategia de recuperación están en [ADR-003](decisions/ADR-003-airflow-spark-execution.md).

### 6.2 Cómo valida CI el DAG

`DagBag` necesita el paquete real de Airflow, que pesa más de un giga.

| Opción | A favor | En contra |
| --- | --- | --- |
| `apache-airflow` pinneado en un requirements de CI | Valida de verdad | CI más lento y más frágil |
| Stub liviano del módulo `airflow` | Rápido | Valida el import, no que el DAG sea válido |
| Construir la imagen en CI | Es la validación más fiel | Lo más caro de las tres |

**Decidido (Andrés, #18):** se instala `apache-airflow==2.10.4` pinneado con el archivo
de constraints de Python 3.12 en un job aparte del CI, y se valida con `DagBag`
(`scripts/validate_dag.py`), no con un stub. El costo medido y los límites están en
[ADR-004](decisions/ADR-004-ci-dag-validation.md).

---

## 7. Enlaces

- Issues de la semana: [#15](https://github.com/AzopG/rapidgo-variant/issues/15) · [#16](https://github.com/AzopG/rapidgo-variant/issues/16) · [#17](https://github.com/AzopG/rapidgo-variant/issues/17) · [#18](https://github.com/AzopG/rapidgo-variant/issues/18) · [#19](https://github.com/AzopG/rapidgo-variant/issues/19)
- Checkpoint de la semana 2: [`semana2.md`](checkpoints/semana2.md)
- Contrato del evento: [`event-contract.md`](event-contract.md)
- KPIs y umbrales: [`kpis-alerts.md`](kpis-alerts.md)
- Decisiones: [ADR-001](decisions/ADR-001-kafka-key-partitions.md) · [ADR-002](decisions/ADR-002-spark-execution-environment.md) · [ADR-003](decisions/ADR-003-airflow-spark-execution.md)
