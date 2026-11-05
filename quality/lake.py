"""Lectura de las tablas Parquet del lake con pyarrow, sin Spark.

Spark escribe cada tabla como una carpeta con `part-*.parquet`, `_SUCCESS` y `.crc`, y
con particiones Hive (`dt=.../hour=...`). `pyarrow.dataset` lee esa carpeta tal cual:
ignora los archivos que empiezan con `_` o `.` y agrega las columnas de particion.

Todo lo demas de `quality` trabaja sobre `Table` (columnas + filas como diccionarios),
asi que las reglas se prueban con tablas en memoria y este es el unico modulo que toca
disco.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Optional


@dataclass(frozen=True)
class Table:
    """Una tabla del lake ya leida: nombres de columna y filas."""

    columns: list[str] = field(default_factory=list)
    rows: list[dict[str, Any]] = field(default_factory=list)

    def __len__(self) -> int:
        return len(self.rows)


# Recibe la ruta de una tabla y devuelve `None` si la carpeta no existe.
Reader = Callable[[Path], Optional[Table]]


def read_table(path: Path) -> Table | None:
    """Lee una tabla Parquet de Spark. `None` si la carpeta no existe.

    Un archivo corrupto o ilegible lanza la excepcion de pyarrow: segun el contrato de
    #16, un error de lectura hace fallar la task para que Airflow reintente.
    """
    path = Path(path)
    if not path.exists():
        return None

    import pyarrow.dataset as ds  # noqa: PLC0415  (solo al leer de verdad)

    # Spark puede guardar timestamps como INT96 (nanosegundos). Leerlos en microsegundos
    # evita que `to_pylist` falle o devuelva tipos de pandas: Spark no guarda mas
    # precision que esa.
    parquet = ds.ParquetFileFormat(
        read_options=ds.ParquetReadOptions(coerce_int96_timestamp_unit="us")
    )
    dataset = ds.dataset(str(path), format=parquet, partitioning="hive")
    table = dataset.to_table()
    return Table(columns=list(table.column_names), rows=table.to_pylist())
