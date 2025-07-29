# etl/etl_core.py

import os
import json
import logging

import pandas as pd
from dbfread import DBF
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.dialects.mysql import insert as mysql_insert

from typing import Callable

# Ruta al archivo de configuración
CONFIG_PATH = "config/config.json"

# Configuración básica de logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)

def cargar_config() -> dict:
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)

def dbf_to_dataframe(dbf_path: str, columns: list = None) -> pd.DataFrame:
    logging.info(f"Leyendo DBF: {dbf_path}")
    table = DBF(dbf_path, load=True, ignore_missing_memofile=True, char_decode_errors="ignore")
    df = pd.DataFrame(iter(table))

    if columns:
        lower_map = {col.lower(): col for col in df.columns}
        actual, missing = [], []
        for col in columns:
            key = col.lower()
            if key in lower_map:
                actual.append(lower_map[key])
            else:
                missing.append(col)
        if missing:
            logging.warning(f"Columnas no encontradas en {os.path.basename(dbf_path)}: {missing}")
        df = df.loc[:, actual]

    return df

def upsert_dataframe(
    df: pd.DataFrame,
    mysql_uri: str,
    table_name: str,
    key_columns: list,
    chunk_size: int = 1000
):
    logging.info(f"Upsert en `{table_name}` — registros: {len(df)} (chunk_size={chunk_size})")
    engine = create_engine(mysql_uri, connect_args={"charset": "utf8mb4"})
    metadata = MetaData()
    table = Table(table_name, metadata, autoload_with=engine)

    records = df.to_dict(orient="records")
    with engine.begin() as conn:
        for i in range(0, len(records), chunk_size):
            chunk = records[i : i + chunk_size]
            stmt = mysql_insert(table)
            update_cols = {
                col.name: stmt.inserted[col.name]
                for col in table.columns
                if col.name not in key_columns and col.name in df.columns
            }
            stmt = stmt.on_duplicate_key_update(**update_cols)
            conn.execute(stmt, chunk)

    logging.info(f"Upsert completo en `{table_name}`.")

def ejecutar_etl(dbf_name: str) -> str:
    """
    Ejecuta el flujo ETL para el DBF indicado (busca en CATALOGS y TRANSACTIONAL)
    y devuelve un mensaje listo para mostrar en la GUI.
    """
    cfg = cargar_config()
    entries = cfg["ENTRIES"].get("CATALOGS", []) + cfg["ENTRIES"].get("TRANSACTIONAL", [])
    entry = next((e for e in entries if e["DBF"].lower() == dbf_name.lower()), None)
    if not entry:
        raise ValueError(f"No existe ENTRY para DBF '{dbf_name}'")

    dbf_path = os.path.join(cfg.get("DBF_DIR", ""), f"{entry['DBF']}.DBF")
    if not os.path.isfile(dbf_path):
        raise FileNotFoundError(f"DBF no encontrado: {dbf_path}")

    mappings = entry["TARGET"]["COLUMNS"]
    source_cols = [m["SOURCE"] for m in mappings]
    df = dbf_to_dataframe(dbf_path, columns=source_cols)

    lower_map = {col.lower(): col for col in df.columns}
    rename_map = {}
    for m in mappings:
        key = m["SOURCE"].lower()
        if key in lower_map:
            rename_map[lower_map[key]] = m["TARGET"]
        else:
            logging.warning(f"Columna DBF no encontrada para mapeo: {m['SOURCE']}")

    df = df.rename(columns=rename_map)

    key_cols = entry.get("KEYS", [])
    missing = set(key_cols) - set(df.columns)
    if missing:
        raise KeyError(f"Faltan claves obligatorias en datos: {missing}")

    table_name = entry["TARGET"]["TABLE"]
    upsert_dataframe(df, cfg["MYSQL_URI"], table_name, key_cols, cfg.get("CHUNK_SIZE", 1000))

    return f"{len(df)} registros sincronizados en '{table_name}'."

def upsert_dataframe_con_progreso(
    df: pd.DataFrame,
    mysql_uri: str,
    table_name: str,
    key_columns: list,
    chunk_size: int,
    progress_callback: Callable[[int], None]
):
    engine = create_engine(mysql_uri, connect_args={"charset":"utf8mb4"})
    metadata = MetaData()
    table = Table(table_name, metadata, autoload_with=engine)

    records = df.to_dict("records")
    total = len(records)
    if total == 0:
        progress_callback(100)
        return

    for i in range(0, total, chunk_size):
        chunk = records[i : i + chunk_size]
        stmt = mysql_insert(table)
        upd = {
            col.name: stmt.inserted[col.name]
            for col in table.columns
            if col.name not in key_columns and col.name in df.columns
        }
        stmt = stmt.on_duplicate_key_update(**upd)
        with engine.begin() as conn:
            conn.execute(stmt, chunk)

        # calcula porcentaje y avanza la barra
        pct = int(((i + len(chunk)) / total) * 100)
        progress_callback(pct)


def ejecutar_etl_con_progreso(
    dbf_name: str,
    chunk_size: int,
    progress_callback: Callable[[int], None]
) -> str:
    """
    Igual que ejecutar_etl, pero usa upsert_dataframe_con_progreso
    para emitir callbacks de progreso.
    """
    cfg = cargar_config()
    entries = cfg["ENTRIES"]["CATALOGS"] + cfg["ENTRIES"]["TRANSACTIONAL"]
    entry = next(e for e in entries if e["DBF"].lower() == dbf_name.lower())

    # Leer y mapear igual que antes...
    dbf_path = os.path.join(cfg["DBF_DIR"], f"{entry['DBF']}.DBF")
    mappings = entry["TARGET"]["COLUMNS"]
    source_cols = [m["SOURCE"] for m in mappings]
    df = dbf_to_dataframe(dbf_path, columns=source_cols)

    # rename case-insensitive
    lower_map = {c.lower(): c for c in df.columns}
    rename_map = {}
    for m in mappings:
        key = m["SOURCE"].lower()
        if key in lower_map:
            rename_map[lower_map[key]] = m["TARGET"]
    df = df.rename(columns=rename_map)

    key_columns = entry.get("KEYS", [])
    table_name  = entry["TARGET"]["TABLE"]

    # aquí usamos la versión con progreso
    upsert_dataframe_con_progreso(
        df, cfg["MYSQL_URI"], table_name, key_columns,
        chunk_size, progress_callback
    )

    return f"{len(df)} registros sincronizados en '{table_name}'."
