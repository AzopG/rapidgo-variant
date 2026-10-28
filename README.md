# RapidGo 360 — Variante 7: Experiencia del cliente

Pipeline Big Data end-to-end que detecta el deterioro de la satisfacción del cliente
durante el servicio de entregas de RapidGo, a partir de calificaciones, motivos de
reclamo y canal de contacto.

**Equipo 7** — Línea de Profundización I (Big Data)
Ingeniería de Sistemas y Computación · Facatativá · 2026

| Integrante | Usuario GitHub |
| --- | --- |
| Andres Maldonado | [FelipeMaldonado10](https://github.com/FelipeMaldonado10) |
| Ivan Palacio | [AzopG](https://github.com/AzopG) |
| Luis Jaraba | [vannysys](https://github.com/vannysys) |

## Problema de negocio

RapidGo no tiene forma de saber, en el momento en que ocurre, que la experiencia del
cliente se está deteriorando. Las calificaciones bajas y los reclamos se descubren
tarde, cuando el pedido ya terminó y el cliente ya decidió no volver a usar la
plataforma.

La variante 7 busca **detectar la caída de satisfacción mientras el servicio ocurre** y
**concentrar los reclamos por motivo, canal y zona**, para que operación pueda actuar
antes de que el deterioro se vuelva una tendencia.

## Alcance de la variante

| Elemento | Definición |
| --- | --- |
| Eventos mínimos | `calificacion`, `motivo`, `canal`, `pedido`, `timestamp` |
| KPIs mínimos | calificación media · motivos frecuentes · tendencia de satisfacción |
| Alertas mínimas | caída de calificación · concentración de reclamos |

Los umbrales iniciales están en [`docs/kpis-alerts.md`](docs/kpis-alerts.md) y se ajustan
con evidencia de ejecución real. El contrato del evento vive en
[`docs/event-contract.md`](docs/event-contract.md) y la decisión de key y particiones en
[`docs/decisions/ADR-001-kafka-key-partitions.md`](docs/decisions/ADR-001-kafka-key-partitions.md).

## Arquitectura

![Arquitectura de la variante 7](docs/arquitectura.png)

```
Generador de eventos (producer)
        |
        v
      Kafka  (topic: rapidgo.ratings, key: order_id)
        |
        v
Spark Structured Streaming
  parseo -> validacion -> transformaciones -> ventanas -> KPIs y alertas
        |
        v
Data Lake: raw/ -> processed/ -> curated/   (Parquet + checkpoint)
        |
        v
Apache Airflow: calidad -> agregacion -> reporte
        |
        v
Indicadores y alertas
```

Diagrama editable: [`docs/arquitectura.png`](docs/arquitectura.png). El bloque de texto
de arriba resume el mismo flujo para quien lee sin imágenes.

## Stack

| Tecnología | Rol |
| --- | --- |
| Python | generador de eventos, pruebas y lógica de negocio |
| Apache Kafka | transporte de eventos en tiempo real |
| Apache Spark Structured Streaming | procesamiento, ventanas, KPIs y alertas |
| Data Lake (Parquet) | persistencia en capas raw / processed / curated |
| Apache Airflow | orquestación, calidad, reintentos y reporte |
| Docker Compose | ejecución local de toda la arquitectura |
| GitHub Actions | integración continua y quality gate |

## Estructura del repositorio

```
rapidgo-variant/
├── .github/workflows/     # CI (y release en Semana 4)
├── airflow/dags/          # DAGs de orquestación
├── producer/              # generador de eventos
├── spark/                 # jobs de Structured Streaming
├── scripts/               # utilidades de infraestructura (topic de Kafka)
├── tests/                 # pruebas de contrato y reglas de negocio
├── samples/               # eventos de ejemplo
├── datalake/              # estructura del data lake (raw/processed/curated)
├── docs/
│   ├── arquitectura.png
│   ├── checkpoints/       # semana1.md, semana2.md, semana3.md
│   └── decisions/         # ADR-001, ADR-002, ADR-003
├── docker-compose.yml
├── requirements.txt
├── README.md
└── .gitignore
```

## Prerrequisitos

- Docker Desktop con Docker Compose v2
- Python 3.13
- Java 17 o superior disponible en el `PATH` (lo requiere PySpark)

## Ejecución local

### 1. Levantar Kafka

```bash
docker compose up -d kafka
```

El contenedor queda `healthy` cuando el broker responde a `kafka-topics.sh`.

### 2. Crear el topic `rapidgo.ratings`

```bash
python -m venv .venv
.venv\Scripts\activate          # Linux/macOS: source .venv/bin/activate
pip install -r requirements.txt

python -m scripts.create_topic
```

Salida esperada la primera vez:

```text
estado observado: topic=rapidgo.ratings exists=False ...
accion: create
rapidgo.ratings: creado
estado final: topic=rapidgo.ratings exists=True partitions=3 replication_factor=1 retention_ms=604800000 ...
```

El script es **idempotente**: en la segunda corrida informa `accion: noop` y no toca
el broker. Si el topic existiera con otra cantidad de particiones o factor de
replicación, falla con un mensaje explícito, porque esas propiedades no se pueden
reducir sin perder datos.

Configuración que impone (ADR-001): 3 particiones, replication-factor 1, retención de
7 días (`604800000` ms) y `cleanup.policy=delete`. La creación automática de topics está
deshabilitada en el broker, así que el topic existe únicamente porque este script lo crea.

Verificación manual:

```bash
docker exec rapidgo-kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --describe --topic rapidgo.ratings
```

### 3. Publicar eventos de calificación

```bash
python -m producer.rating_producer --rate 10 --duration 30 --seed 7
```

| Flag | Default | Para qué |
| --- | --- | --- |
| `--rate` | `5.0` | eventos por segundo |
| `--duration` | `30.0` | segundos de corrida (`--rate` × `--duration` = eventos) |
| `--seed` | aleatoria | semilla: la misma semilla reproduce la misma secuencia |
| `--degradation` | `0.0` | proporción `0..1` de eventos con rating 1–2 y motivo dominante |
| `--dominant-reason` | `late_delivery` | motivo que se concentra en modo degradación |
| `--dry-run` | — | genera y muestra eventos sin publicarlos |

Salida al terminar, con los offsets reales por partición:

```text
publicados=50 particiones_usadas=3 duracion_s=4.9 tasa_efectiva=10.2/s
[p0: count=18 last_offset=17, p1: count=13 last_offset=12, p2: count=19 last_offset=18]
```

Para provocar las alertas A1 y A2 en una corrida integrada:

```bash
python -m producer.rating_producer --rate 10 --duration 60 --seed 7 --degradation 0.7
```

## Pruebas

```bash
pytest -v
```

Las pruebas no necesitan Kafka ni Spark levantados: cubren la lógica pura del proyecto.

## Organización del trabajo

- **Tablero Kanban:** https://github.com/users/AzopG/projects/1
- Columnas: `BACKLOG → READY → IN PROGRESS → REVIEW → DONE`
- Campos: `Semana`, `Responsable`, `Prioridad`, `Componente`
- **Backlog:** 23 Issues abiertos, distribuidos en las cuatro semanas.
- Flujo por cambio: `Issue → Branch → PR → GitHub Actions → Review → Merge`.

## Roadmap de releases

| Release | Semana | Alcance | Estado |
| --- | --- | --- | --- |
| `v0.1.0-foundation` | 1 | Arquitectura, contrato de evento, KPIs, alertas, Kanban, ADR-001 | [publicado](https://github.com/AzopG/rapidgo-variant/releases/tag/v0.1.0-foundation) |
| `v0.2.0-streaming` | 2 | Kafka + Spark Streaming + Data Lake + pruebas + CI | pendiente |
| `v0.3.0-orchestration` | 3 | DAG de Airflow, calidad, recuperación ante fallos, CI ampliado | pendiente |
| `v1.0.0` | 4 | Quality gate, README final, release y video | pendiente |

## Equipo y contribuciones

La tabla de contribuciones (responsabilidades, Issues y PR principales por integrante)
se completa en la Semana 4, a partir del historial real del repositorio.

## Enlaces

- Video final: _pendiente_
- Release actual: [`v0.1.0-foundation`](https://github.com/AzopG/rapidgo-variant/releases/tag/v0.1.0-foundation)
- Tablero Kanban: https://github.com/users/AzopG/projects/1
- Contrato del evento: [`docs/event-contract.md`](docs/event-contract.md)
- KPIs y alertas: [`docs/kpis-alerts.md`](docs/kpis-alerts.md)
