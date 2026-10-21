# Checkpoint Semana 1 — RapidGo 360, Variante 7

**Equipo 7:** Andres Maldonado · Ivan Palacio · Luis Jaraba
**Release:** `v0.1.0-foundation` · **Fecha:** 2026-10-22

## 1. Qué quedó funcionando

- Repositorio público con los tres integrantes como colaboradores.
- Backlog de 23 Issues distribuido en las cuatro semanas, cargado en el tablero Kanban
  con columnas BACKLOG / READY / IN PROGRESS / REVIEW / DONE y los campos Semana,
  Responsable, Prioridad y Componente.
- Contrato del evento `customer_rating` documentado, con evento de ejemplo en `samples/`.
- Tres KPIs y dos alertas con umbrales iniciales justificados.
- ADR-001 con la decisión de key, particiones y retención del topic.
- `docker-compose.yml` preliminar con Kafka en modo KRaft (sin ZooKeeper).
- Protección de `main`: PR obligatorio, 1 aprobación, dismiss de aprobaciones viejas.

## 2. Evidencia real observada

- `docker compose config --quiet` sobre el compose preliminar: exit code 0.
- `gh project item-list`: 23 items en el tablero, todos con Semana y Componente.
- PR #24: review `CHANGES_REQUESTED` sobre `pedido_id` → corrección en `200f99b` →
  `APPROVED` → merge `b9c4958`.
- Segunda verificación del tablero: 23 de 23 items (la primera carga dejó 19).

## 3. Decisión técnica más importante

Key `order_id`, 3 particiones y retención de 7 días (ADR-001). Un pedido genera una sola
calificación, así la deduplicación queda dentro de una partición y Spark puede escalar a
3 consumers sin recrear el topic.

## 4. Problema real y cómo se resolvió

Al cargar el tablero, la API devolvió éxito para los 23 items pero solo 19 persistieron
(faltaban #16, #17, #19 y #20). Se detectó comparando el conteo devuelto por
`gh project item-list` (19) contra el esperado (23). Se corrigió con un script que
reinserta únicamente esos cuatro y vuelve a verificar el conteo (23/23). Aprendizaje: el
OK de una API no es evidencia, se verifica contra la fuente.

## 5. Riesgo principal

Integrar Kafka + Spark en Docker sobre Windows con Python 3.13. PySpark 4.0 y el conector
`spark-sql-kafka-0-10` tienen que coincidir en versión, y `ADVERTISED_LISTENERS` apunta a
`localhost:9092`, que sirve para procesos en el host pero no para un contenedor de Spark.
Mitigación: en la Semana 2 el producer y Spark corren en el host con `venv`.

## 6. Plan de la Semana 2

1. Producer y topic con 3 particiones (#6, #7).
2. Spark: parseo, validación y 3 transformaciones (#8).
3. Parquet por capas con checkpoint (#9).
4. Alertas A1 y A2 sobre ventanas (#10, #11).
5. Tests, CI y checkpoint de la semana (#12, #13, #14).

## 7. Enlaces

- Tablero: https://github.com/users/AzopG/projects/1
- Contrato: [`docs/event-contract.md`](../event-contract.md)
- KPIs y alertas: [`docs/kpis-alerts.md`](../kpis-alerts.md)
- ADR-001: [`docs/decisions/ADR-001-kafka-key-partitions.md`](../decisions/ADR-001-kafka-key-partitions.md)
- PR de arquitectura: https://github.com/AzopG/rapidgo-variant/pull/24
