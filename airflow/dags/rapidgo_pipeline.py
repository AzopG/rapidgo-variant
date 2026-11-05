"""DAG de orquestacion del pipeline RapidGo 360 (Issue #15).

Flujo por corrida:

    iniciar_corrida -> crear_topic -> generar_eventos -> ejecutar_ingesta
                                                          |-> calidad_raw
                                                          -> ejecutar_alertas -> calcular_kpis -> calidad_curated
    [calidad_raw, calidad_curated] -> generar_reporte

Cada corrida usa su propio topic, lake y checkpoints (`orchestration.pipeline`), asi
que dos corridas no comparten estado y los conteos del reporte son reproducibles.

Las tasks de calidad se importan dentro de la funcion, no a nivel de modulo: #16 las
escribe Luis y el DAG tiene que seguir siendo importable mientras no existan. Cuando
falta el modulo, falla la task con un `ModuleNotFoundError` explicito, no el parseo del
DAG.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from orchestration.pipeline import (
    ALERTS_JOB,
    INGEST_JOB,
    KPI_JOB,
    RunContext,
    build_context,
    producer_command,
    run_command,
    spark_env,
    spark_job_command,
    topic_command,
)

PRODUCER_RATE = float(os.environ.get("RAPIDGO_PRODUCER_RATE", "20"))
PRODUCER_DURATION = float(os.environ.get("RAPIDGO_PRODUCER_DURATION", "15"))
# 120 min: K3 compara los ultimos 60 min contra los 60 anteriores. Sin historico, la
# tendencia no tiene base y el reporte de #16 no puede validarla.
BACKFILL_MINUTES = float(os.environ.get("RAPIDGO_BACKFILL_MINUTES", "120"))
RETRIES = int(os.environ.get("RAPIDGO_DAG_RETRIES", "2"))

DEFAULT_ARGS = {
    "owner": "equipo-7",
    "retries": RETRIES,
    "retry_delay": timedelta(minutes=1),
    "depends_on_past": False,
}


def _run_context(ti) -> RunContext:
    """Recupera el contexto que dejo `iniciar_corrida` en XCom."""
    payload = ti.xcom_pull(task_ids="iniciar_corrida")
    if not payload:
        raise RuntimeError(
            "falta el contexto de la corrida: la task 'iniciar_corrida' no dejo XCom"
        )
    return RunContext.from_xcom(payload)


def iniciar_corrida(**_):
    """Crea el contexto de la corrida y las carpetas de lake y checkpoints."""
    ctx = build_context()
    ctx.lake_path.mkdir(parents=True, exist_ok=True)
    ctx.checkpoint_path.mkdir(parents=True, exist_ok=True)
    print(
        f"CORRIDA {ctx.run_id} topic={ctx.topic} lake={ctx.lake_dir} "
        f"checkpoints={ctx.checkpoint_dir}",
        flush=True,
    )
    return ctx.as_xcom()


def crear_topic(ti, **_):
    """Crea el topic de la corrida con la configuracion de ADR-001."""
    ctx = _run_context(ti)
    run_command(topic_command(ctx))
    return {"topic": ctx.topic}


def generar_eventos(ti, **_):
    """Publica los eventos de la corrida, repartidos en la ventana de backfill."""
    ctx = _run_context(ti)
    run_command(
        producer_command(
            ctx,
            rate=PRODUCER_RATE,
            duration=PRODUCER_DURATION,
            backfill_minutes=BACKFILL_MINUTES,
        )
    )
    return {"topic": ctx.topic, "backfill_minutes": BACKFILL_MINUTES}


def ejecutar_ingesta(ti, **_):
    """Corre el job de ingesta una vez y deja el lake de la corrida."""
    ctx = _run_context(ti)
    run_command(spark_job_command(ctx, INGEST_JOB, ["--once"]), env=spark_env())
    return {"lake_dir": ctx.lake_dir}


def ejecutar_alertas(ti, **_):
    """Evalua A1 y A2 sobre el lake de la corrida."""
    ctx = _run_context(ti)
    run_command(spark_job_command(ctx, ALERTS_JOB, ["--once"]), env=spark_env())
    return {"lake_dir": ctx.lake_dir}


def calcular_kpis(ti, **_):
    """Agrega K1, K2 y K3 sobre `processed` (la agregacion del objetivo de #15)."""
    ctx = _run_context(ti)
    run_command(
        spark_job_command(ctx, KPI_JOB, streaming=False), env=spark_env()
    )
    return {"lake_dir": ctx.lake_dir}


def calidad_raw(ti, **_):
    """Valida `raw` y `processed`: vacios, esquema y duplicados (#16)."""
    ctx = _run_context(ti)
    from quality.checks import check_raw  # noqa: PLC0415

    result = check_raw(ctx.lake_dir)
    print(f"CALIDAD raw ok={result.get('ok')}", flush=True)
    return result


def calidad_curated(ti, **_):
    """Valida `curated`: los tres KPIs presentes y en rango (#16)."""
    ctx = _run_context(ti)
    from quality.checks import check_curated  # noqa: PLC0415

    result = check_curated(ctx.lake_dir)
    print(f"CALIDAD curated ok={result.get('ok')}", flush=True)
    return result


def generar_reporte(ti, **_):
    """Escribe el reporte con KPIs, alertas y resultado de calidad (#16)."""
    ctx = _run_context(ti)
    from quality.report import build_report  # noqa: PLC0415

    quality = {
        "raw": ti.xcom_pull(task_ids="calidad_raw"),
        "curated": ti.xcom_pull(task_ids="calidad_curated"),
    }
    path = build_report(ctx.lake_dir, quality)
    print(f"REPORTE {path}", flush=True)
    return {"report": path}


with DAG(
    dag_id="rapidgo_pipeline",
    description="Ingesta, alertas, KPIs y calidad sobre el lake de RapidGo 360",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["rapidgo", "semana3"],
) as dag:
    iniciar = PythonOperator(task_id="iniciar_corrida", python_callable=iniciar_corrida)
    topic = PythonOperator(task_id="crear_topic", python_callable=crear_topic)
    eventos = PythonOperator(task_id="generar_eventos", python_callable=generar_eventos)
    ingesta = PythonOperator(task_id="ejecutar_ingesta", python_callable=ejecutar_ingesta)
    alertas = PythonOperator(task_id="ejecutar_alertas", python_callable=ejecutar_alertas)
    kpis = PythonOperator(task_id="calcular_kpis", python_callable=calcular_kpis)
    raw = PythonOperator(task_id="calidad_raw", python_callable=calidad_raw)
    curated = PythonOperator(task_id="calidad_curated", python_callable=calidad_curated)
    reporte = PythonOperator(task_id="generar_reporte", python_callable=generar_reporte)

    iniciar >> topic >> eventos >> ingesta
    ingesta >> alertas >> kpis
    ingesta >> raw
    kpis >> curated
    [raw, curated] >> reporte
