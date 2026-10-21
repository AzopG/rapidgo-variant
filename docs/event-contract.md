# Contrato de evento — `customer_rating` (Variante 7)

Evento de calificación que el cliente deja al finalizar una entrega. Es la única
fuente del pipeline: de acá salen los tres KPIs y las dos alertas.

- **Topic:** `rapidgo.ratings`
- **Formato:** JSON UTF-8, un evento por mensaje
- **Key:** `order_id`
- **Versión del contrato:** `1` (campo `event_version`)

## Esquema

| Campo | Tipo | Obligatorio | Descripción |
| --- | --- | --- | --- |
| `event_id` | string (UUID v4) | sí | Identificador único del evento. Permite deduplicar en Spark. |
| `event_type` | string | sí | Siempre `customer_rating`. |
| `event_version` | integer | sí | Versión del contrato. Hoy `1`. |
| `occurred_at` | string (ISO-8601 UTC) | sí | Momento en que el cliente calificó. Base de todas las ventanas. |
| `order_id` | string | sí | Pedido calificado. Es la key de Kafka y la unidad de deduplicación. |
| `customer_id` | string | sí | Cliente que califica. Se usa para no contar dos veces el mismo pedido. |
| `courier_id` | string | no | Repartidor asignado. Permite segmentar la tendencia. |
| `restaurant_id` | string | no | Comercio del pedido. Permite cruzar con la variante 6. |
| `zone_id` | string | sí | Zona de entrega (`Z-NORTE`, `Z-SUR`, ...). Necesaria para concentrar reclamos. |
| `channel` | enum | sí | Canal por el que llega la calificación: `app`, `web`, `call_center`, `chat`. |
| `rating` | integer 1–5 | sí | Calificación del cliente. 1 = peor, 5 = mejor. |
| `complaint_reason` | enum o `null` | condicional | Obligatorio cuando `rating <= 3`; `null` cuando `rating >= 4`. |
| `comment_length` | integer | no | Largo del comentario libre. No se versiona el texto por privacidad. |
| `delivery_minutes` | number | no | Duración real de la entrega. Contexto para explicar una caída. |
| `order_value` | number | no | Valor del pedido. Contexto para segmentar por ticket. |
| `ingested_at` | string o `null` | no | Lo completa Spark al persistir; el producer lo envía en `null`. |

### Enumeración `complaint_reason`

`late_delivery` · `cold_food` · `missing_item` · `wrong_item` ·
`courier_attitude` · `app_issue` · `payment_issue` · `other`

### Enumeración `channel`

`app` · `web` · `call_center` · `chat`

## Reglas de validación

Un evento se descarta (y se cuenta en `datalake/curated/ratings_rejected`) si:

1. Falta cualquiera de los campos obligatorios.
2. `rating` está fuera de `1..5`.
3. `occurred_at` no es una fecha ISO-8601 válida o es futura en más de 5 minutos.
4. `channel` o `complaint_reason` no pertenecen a la enumeración.
5. `rating <= 3` y `complaint_reason` es `null`.
6. `rating >= 4` y `complaint_reason` no es `null` (se normaliza a `null`, no se descarta).

## Ejemplo

Ver [`samples/rating_event.json`](../samples/rating_event.json).

## Por qué estos campos

La guía exige como mínimo `calificación`, `motivo`, `canal`, `pedido` y
`timestamp`. Se agregan `zone_id` y `delivery_minutes` porque sin ellos la alerta
de "concentración de reclamos" no puede ubicar el problema, y la "tendencia de
satisfacción" no puede explicarse. El resto es contexto opcional que no rompe el
contrato si falta.
