# ADR-001 — Key y particiones del topic `rapidgo.ratings`

**Estado:** aceptado
**Fecha:** 2026-10-22
**Responsable:** Equipo 7

## Contexto / problema

El producer publica eventos `customer_rating` en Kafka y Spark Structured Streaming
los consume. Necesitamos decidir la **key**, el **número de particiones** y la
**retención** del topic antes de escribir producción. La decisión afecta tres cosas:

1. El orden en que Spark ve los eventos de un mismo pedido.
2. La deduplicación de calificaciones duplicadas.
3. La capacidad de escalar el consumo sin rehacer el topic.

## Opciones consideradas

| Opción | Key | Ventaja | Problema |
| --- | --- | --- | --- |
| A | `order_id` | Orden y deduplicación por pedido; un pedido cae siempre en la misma partición | Baja cardinalidad por ventana, posibles particiones calientes si los pedidos se concentran |
| B | `zone_id` | Co-localiza todo lo de una zona; útil para alertas por zona | Un pedido puede tener varios eventos que se reordenan; deduplicación por pedido se vuelve global |
| C | `customer_id` | Agrupa el historial del cliente | Un cliente puede calificar varios pedidos; no aísla el pedido |
| D | Sin key (round-robin) | Reparte parejo | Pierde orden por pedido y obliga a deduplicar en ventanas amplias |

## Decisión tomada

- **Key:** `order_id`
- **Particiones:** `3`
- **Factor de replicación:** `1` en local (Docker Compose); `3` en un clúster real
- **Retención:** `7 días` (`retention.ms=604800000`), `cleanup.policy=delete`

Configuración de creación del topic:

```
kafka-topics.sh --create \
  --topic rapidgo.ratings \
  --partitions 3 \
  --replication-factor 1 \
  --config retention.ms=604800000 \
  --config cleanup.policy=delete
```

## Evidencia utilizada

- Un pedido genera **una sola calificación** en el modelo de negocio; la key
  `order_id` hace que la deduplicación sea un `dropDuplicates(["event_id", "order_id"])`
  barato dentro de una partición.
- El volumen esperado de la demo es de **~5 eventos/min en hora pico** (generador de
  la Semana 2). Con 3 particiones, cada una queda muy por debajo de su capacidad y
  permite que Spark lea en paralelo con `minPartitions = 3`.
- 3 particiones es el mínimo que permite **escalar a 3 consumers** (un consumer por
  partición) sin recrear el topic, que es el mismo tamaño del equipo.
- Retención de 7 días: alcanza para reprocesar los datos de la demo y para que
  Airflow pueda releer una ventana completa si un DAG falla y se reintenta.

**Pendiente de validar en Semana 2:** confirmar con `kafka-consumer-groups.sh
--describe` que el lag se mantiene en cero con 3 particiones y 3 consumers, y medir
si el desbalance entre particiones es aceptable. Si aparece una partición caliente,
se re-evalúa con `order_id` + salting (ver consecuencias).

## Consecuencias / trade-offs

- **A favor:** orden y deduplicación por pedido, consumo paralelo hasta 3 consumers,
  nada de estado global en Spark para el caso principal.
- **En contra:** si el generador produce pedidos con `order_id` muy sesgado
  (por ejemplo, todos `ord-0000xx`), una partición concentra la carga. La mitigación
  documentada es un prefijo de salting en la key (`{0..2}#order_id`), que se adopta
  solo si la medición de la Semana 2 lo justifica.
- **Consecuencia operativa:** la alerta A2 por zona no puede apoyarse en la
  partición, así que se calcula con `groupBy(zone_id)` en Spark, no con el particionado.
