# Imagen de Airflow con lo que el DAG necesita para correr Spark adentro (ADR-003).
#
# Decision: los jobs de PySpark se ejecutan en este mismo contenedor, con
# `--master local[*]`. No se monta el socket de Docker ni se hace docker-in-docker.
# Eso exige dos cosas que la imagen oficial no trae: Java 17 (PySpark 4.0 no arranca
# sin el) y el wheel de PySpark.
FROM apache/airflow:2.10.4-python3.12

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
        curl \
        procps \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow

COPY --chown=airflow:root docker/airflow/requirements.txt /tmp/requirements-airflow.txt
RUN pip install --no-cache-dir -r /tmp/requirements-airflow.txt

# Cache de Ivy: la primera corrida resuelve el conector de Kafka desde Maven Central y
# las siguientes lo reusan. El volumen `airflow_ivy` del compose lo persiste.
RUN mkdir -p /opt/airflow/.ivy2
