# KPIs y reglas de alerta — Variante 7

Los tres KPIs y las dos alertas se calculan con **Spark Structured Streaming** sobre
`samples`/Kafka y se orquestan con Airflow para el reporte. Todos los umbrales son
**iniciales**: se ajustan con la evidencia de la Semana 2 y el ajuste se documenta.

## Objetivo de servicio

La meta operativa de RapidGo para esta variante es **calificación media ≥ 4.0** por
ventana. Todos los umbrales se derivan de esa meta.

## KPIs

| ID | KPI | Definición | Ventana | Salida |
| --- | --- | --- | --- | --- |
| K1 | Calificación media | `avg(rating)` global y por `zone_id`, `channel` y `courier_id`. | 10 min, deslizante cada 1 min | `curated/kpi_rating_avg` |
| K2 | Motivos frecuentes | Top 5 de `complaint_reason` con su participación (`count / total_reclamos`). | 15 min, deslizante cada 1 min | `curated/kpi_top_reasons` |
| K3 | Tendencia de satisfacción | `avg(rating)` de los últimos 60 min comparado con los 60 min anteriores; se reporta delta y porcentaje. | 60 min vs 60 min previos | `curated/kpi_trend` |

Clasificación de tendencia (K3):

| Delta | Estado |
| --- | --- |
| `> -0.20` y `< 0.20` | estable |
| `<= -0.20` | deterioro |
| `>= 0.20` | mejora |

## Alertas

| ID | Alerta | Condición | Severidad |
| --- | --- | --- | --- |
| A1 | Caída de calificación | `avg(rating) < 3.60` con `n >= 20` eventos en 10 min | `warning` |
| A1 | Caída de calificación | `avg(rating) < 3.20` con `n >= 20` eventos en 10 min | `critical` |
| A2 | Concentración de reclamos | Un `complaint_reason` representa `>= 30%` de los reclamos, con `n_reclamos >= 10` en 15 min | `warning` |
| A2 | Concentración de reclamos | Ese motivo representa `>= 50%` de los reclamos, con `n_reclamos >= 10` en 15 min | `critical` |

Reglas transversales:

- **Ventana por `occurred_at`**, no por hora de llegada: una calificación vieja no
  debe disparar una alerta de ahora.
- **Mínimo de muestra** (`n >= 20` en A1, `n_reclamos >= 10` en A2): evita alertas
  por dos o tres eventos.
- **Cooldown de 15 minutos** por `(alerta, zona, motivo)`: una condición sostenida
  genera una alerta, no una por micro-batch.
- A2 se evalúa también agrupada por `zone_id`; la alerta incluye la zona.
- Toda alerta se persiste en `curated/alerts` con la ventana, el valor observado y
  el umbral que la disparó, para poder auditar por qué sonó.

## Justificación de los umbrales iniciales

| Umbral | Valor | Por qué |
| --- | --- | --- |
| Meta 4.0 | — | Calificación media de una plataforma de entregas sana; por debajo el cliente empieza a migrar. |
| A1 warning 3.60 | −0.40 vs meta | Deja margen para ruido de muestra pequeña sin avisar por una ventana normal. |
| A1 critical 3.20 | −0.80 vs meta | Una media de 3.2 equivale a que la mayoría califique 3 o menos: el servicio está roto. |
| A1 mínimo 20 eventos | — | Con 10 min de operación normal, 20 eventos ya descartan un pico aislado. |
| A2 30% / 50% | — | Un motivo concentrando un tercio de los reclamos ya es accionable; la mitad indica un incidente puntual, no una queja difusa. |

**Cómo se ajustan:** en la Semana 2 se ejecuta el pipeline con el generador
sembrando una degradación controlada (proporción de ratings 1–2 y motivo dominante
crecientes). Se registran los valores observados y se mueve el umbral solo si la
corrida demuestra falsos positivos o falsos negativos. Cada ajuste queda en
`docs/checkpoints/semana2.md` con el valor antes, el valor después y el log que lo
justifica.
