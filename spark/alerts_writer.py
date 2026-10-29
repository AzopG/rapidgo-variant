"""Persistencia de alertas en el lake.

Las dos queries de `spark/alerts_job.py` (A1 y A2) escriben en la misma ruta
`curated/alerts`, cada una desde su propio hilo de micro-batch. Spark no soporta
dos escritores concurrentes sobre una misma salida: el committer de cada job usa
el mismo directorio `_temporary/0` debajo de la raiz, y el que termina primero se
lo lleva puesto al de al lado. El sintoma tipico es `ParentNotDirectoryException`
o `FileNotFoundException` sobre un `part-*` recien escrito, y se pierden filas.

Particionar por `alert_id` no alcanza: medido, la colision esta en `_temporary/0`,
que es unico por ruta de salida, no por particion. La unica forma de que no haya
dos committers a la vez es serializar la escritura, que es lo que hace
`append_alerts`.
"""

from __future__ import annotations

import threading
from pathlib import Path
from typing import Any, Iterable

# Un lock por proceso: el job de alertas es un unico proceso con dos queries dentro.
_WRITE_LOCK = threading.Lock()


def append_alerts(spark: Any, path: Path, records: Iterable[Any], schema: Any) -> None:
    """Agrega las alertas de un micro-batch a `path`. No hace nada si no hay filas."""
    rows = list(records)
    if not rows:
        return
    with _WRITE_LOCK:
        spark.createDataFrame(rows, schema).write.mode("append").parquet(str(path))
