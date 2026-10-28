# ADR-002 — Entorno de ejecución de Spark: contenedor Linux

- **Estado:** aceptado
- **Fecha:** 2026-10-27
- **Semana:** 2
- **Issues:** #8 (job de Spark), #14 (ADR-002)

## Contexto

La Semana 1 dejó anotado como riesgo principal integrar Kafka y Spark sobre Windows con
Python 3.13: PySpark 4.0, el conector de Kafka y las versiones de Java tienen que
coincidir, y el `ADVERTISED_LISTENERS` del compose apuntaba a `localhost`.

Antes de escribir el job de streaming se midió el entorno real. El host de desarrollo es
Windows con Python 3.13.3, Java 20 (sin `JAVA_HOME`) y un `venv` con `pyspark==4.0.1`.

## Evidencia

### 1. Sesión Spark en el host: funciona

```text
SPARK_VERSION: 4.0.1
```

Levanta con Java 20 y lee archivos locales con esquema explícito.

### 2. Escritura de Parquet en el host: falla

```text
PARQUET_FAIL: Py4JJavaError  at org.apache.hadoop.util.Shell.getWinUtilsPath(Shell.java:789)
```

Hadoop no puede escribir en el sistema de archivos local de Windows sin `winutils.exe`
y `HADOOP_HOME`. No es un problema de Spark ni de Parquet: es el `RawLocalFileSystem`.

### 3. Conector de Kafka en el host: falla por la misma causa

```text
RuntimeException: java.io.FileNotFoundException:
HADOOP_HOME and hadoop.home.dir are unset
```

Cargar `spark-sql-kafka-0-10` inicializa clases de Hadoop que disparan la misma
validación. Con el conector en el classpath, Spark ni siquiera crea la sesión.

### 4. Lectura del sample: trampa de formato

`samples/rating_event.json` está indentado en 18 líneas. El reader de Spark espera
JSON delimitado por líneas:

```text
default:        filas: 18 | no nulas: 0
multiLine=True: filas: 1  | no nulas: 1
```

Un job que no fije `multiLine` o que use un sample indentado va a procesar 18 filas
nulas sin error visible.

### 5. Contenedor Linux: extremo a extremo

`apache/spark:4.0.1` (Java 17, Python 3.10) contra el Kafka del compose:

```text
SPARK_VERSION: 4.0.1
KAFKA_SCHEMA: struct<key:binary,value:binary,topic:string,partition:int,
              offset:bigint,timestamp:timestamp,timestampType:int>
EVENTS_BATCHES: 1
EVENTS_ROWS: 50
window_start        window_end          avg_rating  n
2026-10-27 17:00:00 2026-10-27 17:10:00 2.6         50
```

Cincuenta eventos publicados por el producer, leídos desde `kafka:29092`, parseados,
agregados por ventana de 10 minutos y persistidos en Parquet dentro de
`datalake/processed/`.

### 6. Wrapper verificado

`python -m scripts.run_spark_job spark/<job>.py` construye el `docker run` con todas
estas reglas. Corrido contra un job de prueba:

```text
SMOKE_ROWS: 50
|partition|count|
|        0|   18|
|        1|   13|
|        2|   19|
```

Los mismos conteos que reportó el producer al publicar, leídos ahora desde Spark.

## Decisión

**Spark corre en un contenedor Linux (`apache/spark:4.0.1`). El producer sigue corriendo
en el host**, porque publica por `localhost:9092` y no necesita el sistema de archivos de
Hadoop.

Cuatro reglas operativas que se derivan de la prueba:

1. **Listener `INTERNAL`.** El contenedor usa `kafka:29092`. `localhost:9092` dentro del
   contenedor apunta a sí mismo, no al broker.
2. **`--conf spark.jars.ivy=/tmp/.ivy2`.** La imagen oficial tiene `HOME=/nonexistent`, así
   que Ivy no puede escribir su caché y `--packages` aborta con
   `FileNotFoundException: /nonexistent/.ivy2.5.2/cache/...`.
3. **El repo se monta en `/opt/rapidgo`.** El job escribe en `datalake/processed/` y los
   checkpoints en `checkpoints/`, ambos fuera del código versionado.
4. **Ventanas en modo `update` + `foreachBatch`.** Con `outputMode("append")` una ventana
   solo se emite cuando el watermark pasa su fin: una corrida de 30 segundos produce
   **cero** filas. Verificado: `KPI_ROWS: 0`. Con `update` + `foreachBatch` la misma
   corrida emitió su ventana con `n=50`.

## Alternativas descartadas

### Instalar `winutils.exe` en cada máquina

Es la solución más difundida en Windows. Se descarta por tres motivos: es un binario de
terceros que cada integrante tiene que descargar y ubicar a mano, su versión debe
coincidir con el `hadoop-client` que trae Spark 4, y no reproduce el entorno del
docente. Un pipeline que solo corre en la máquina de quien lo escribió no es un
pipeline reproducible.

### Bajar a Python 3.11 + Spark 3.5

Era el plan de contingencia de la Semana 1. No hace falta: Spark 4.0.1 funciona en el
contenedor con Python 3.10 y Java 17, que son las versiones de la imagen oficial. Se
mantiene como contingencia si el conector fallara.

### WSL2 con Spark nativo

Funciona, pero agrega un sistema operativo más a la matriz de soporte y no lo puede
replicar quien no tenga WSL. El contenedor corre igual en Windows, Linux y macOS porque
todos usan el mismo Docker que ya se necesita para Kafka.

## Consecuencias

- **Positivas:** mismo entorno para los tres integrantes; sin binarios de terceros; el
  pipeline completo se levanta con Docker; `docker compose config` sigue siendo la única
  validación de infraestructura que corre en CI sin Docker.
- **Negativas:** el ciclo de edición es más lento que un `python job.py` local, porque
  cada corrida arranca un contenedor. Se mitiga montando el repo como volumen, sin
  reconstruir imagen. Además, cada contenedor resuelve el conector desde Maven Central al
  arrancar (unos segundos); cachearlo en un volumen quedó fuera de alcance porque el
  volumen lo crea Docker como `root` y el contenedor corre como `spark`.
- **Riesgo residual:** la descarga del conector necesita red. En la corrida local no es un
  problema, pero si se necesita CI sin red habría que pre-empaquetar los jars.
- El job de negocio (`spark/streaming_job.py`) queda a cargo de Andrés (Issue #8). Este
  ADR fija el entorno, no la lógica.

## Referencias

- `docs/checkpoints/semana1.md` — riesgo declarado en la Semana 1.
- `docs/decisions/ADR-001-kafka-key-partitions.md` — key y particiones.
- `docker-compose.yml` — listener `INTERNAL` y broker.
- `scripts/run_spark_job.py` — wrapper que aplica estas reglas.
