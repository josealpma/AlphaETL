# etl/etl_core.py

import os
import json
import logging
import hashlib
from datetime import datetime
from typing import Callable

import pandas as pd
from dbfread import DBF
from sqlalchemy import create_engine, MetaData, Table, select
from sqlalchemy.dialects.mysql import insert as mysql_insert

from etl.control import actualizar_fecha

import psutil, time

# Rutas de configuración
CONFIG_PATH = "config/config.json"
SCHEMA_PATH = "config/schemas.json"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

def cargar_config() -> dict:
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)

def cargar_schemas() -> dict:
    with open(SCHEMA_PATH, "r", encoding="utf-8") as f:
        return json.load(f)

def calcular_hash_fila(row: dict, cols: list) -> str:
    s = "|".join(str(row.get(c, "")) for c in cols)
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

def dbf_to_dataframe(dbf_path: str, columns: list = None) -> pd.DataFrame:
    logging.info(f"Leyendo DBF: {dbf_path}")
    table = DBF(dbf_path, load=True, ignore_missing_memofile=True, char_decode_errors="ignore")
    df = pd.DataFrame(iter(table))
    if columns:
        lower = {c.lower(): c for c in df.columns}
        sel = [lower[c.lower()] for c in columns if c.lower() in lower]
        missing = [c for c in columns if c.lower() not in lower]
        if missing:
            logging.warning(f"Columnas no encontradas en {os.path.basename(dbf_path)}: {missing}")
        df = df.loc[:, sel]
    return df

def filter_new_or_changed(
    df: pd.DataFrame,
    engine,
    table_name: str,
    key_field: str,
    hash_field: str,
    hash_cols: list
) -> pd.DataFrame:
    # 1) calculamos hash
    records = df.to_dict("records")
    df[hash_field] = [calcular_hash_fila(r, hash_cols) for r in records]

    # 2) traemos hashes existentes
    metadata = MetaData()
    tbl = Table(table_name, metadata, autoload_with=engine)
    stmt = select(tbl.c[key_field], tbl.c[hash_field])\
           .where(tbl.c[key_field].in_(df[key_field].tolist()))

    with engine.connect() as conn:
        existing = dict(conn.execute(stmt).fetchall())

    # 3) filtramos
    mask = df.apply(lambda r: existing.get(r[key_field]) != r[hash_field], axis=1)
    return df.loc[mask]

def upsert_dataframe_con_progreso(
    df: pd.DataFrame,
    mysql_uri: str,
    table_name: str,
    key_columns: list,
    chunk_size: int,
    progress_callback: Callable[[int], None]
):
    engine = create_engine(mysql_uri, connect_args={"charset": "utf8mb4"})
    meta = MetaData()
    tbl = Table(table_name, meta, autoload_with=engine)

    recs = df.to_dict("records")
    total = len(recs)
    if total == 0:
        progress_callback(100)
        return

    for i in range(0, total, chunk_size):
        chunk = recs[i : i + chunk_size]
        stmt = mysql_insert(tbl)
        upd = {
            c.name: stmt.inserted[c.name]
            for c in tbl.columns
            if c.name not in key_columns and c.name in df.columns
        }
        stmt = stmt.on_duplicate_key_update(**upd)
        with engine.begin() as conn:
            conn.execute(stmt, chunk)

        pct = int(((i + len(chunk)) / total) * 100)
        progress_callback(pct)

def ejecutar_etl_con_progreso(
    dbf_name: str,
    chunk_size: int,
    progress_callback: Callable[[int], None]
) -> str:
    cfg = cargar_config()
    schemas = cargar_schemas()
    entries = schemas["ENTRIES"]["CATALOGS"] + schemas["ENTRIES"]["TRANSACTIONAL"]
    entry = next(e for e in entries if e["DBF"].lower() == dbf_name.lower())

    # 1) lee DBF (solo columnas SOURCE)
    src_cols = [m["SOURCE"] for m in entry["TARGET"]["COLUMNS"]]
    dbf_path = os.path.join(cfg["DBF_DIR"], f"{entry['DBF']}.DBF")
    df = dbf_to_dataframe(dbf_path, src_cols)

    # 2) rename SOURCE→TARGET
    lower = {c.lower(): c for c in df.columns}
    rename = {
        lower[m["SOURCE"].lower()]: m["TARGET"]
        for m in entry["TARGET"]["COLUMNS"]
        if m["SOURCE"].lower() in lower
    }
    df = df.rename(columns=rename)

    # 3) determinamos columnas de hash y llave
    hash_cols = entry["TARGET"].get("HASHES", [])
    # si no hay KEYS definido, usamos la primera de HASHES como key_field
    key_columns = entry.get("KEYS") or [hash_cols[0]]
    key_field = key_columns[0]

    # 4) filtrado incremental por hash
    engine = create_engine(cfg["MYSQL_URI"], connect_args={"charset": "utf8mb4"})
    df_proc = filter_new_or_changed(
        df, engine,
        entry["TARGET"]["TABLE"],
        key_field,
        "row_hash",
        hash_cols
    )

    # 5) upsert con progreso
    upsert_dataframe_con_progreso(
        df_proc,
        cfg["MYSQL_URI"],
        entry["TARGET"]["TABLE"],
        key_columns,
        chunk_size,
        progress_callback
    )

    # 6) actualiza fecha de última sync
    actualizar_fecha(dbf_name, datetime.now().isoformat(sep=" ", timespec="seconds"))

    return f"{len(df_proc)} filas nuevas o modificadas sincronizadas en '{entry['TARGET']['TABLE']}'."
