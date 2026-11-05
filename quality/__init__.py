"""Calidad del lake y reporte de la corrida (Issue #16).

Python puro, sin Airflow: el DAG `rapidgo_pipeline` llama a `quality.checks` y
`quality.report`, y estas funciones se prueban con `pytest` sin levantar nada.
"""
