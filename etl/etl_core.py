# en etl/etl_core.py
import os, sys, json
import logging
import hashlib
import time
from datetime import datetime
from typing import Callable, List, Tuple

import psutil
import pandas as pd
from dbfread import DBF
from sqlalchemy import create_engine, MetaData, Table, select, text
from sqlalchemy.dialects.mysql import insert as mysql_insert

from etl.control import actualizar_fecha

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

if getattr(sys, "frozen", False):
    BASE_DIR = sys._MEIPASS
else:
    # sys.argv[0] apunta al script principal (main.py)
    BASE_DIR = os.path.dirname(os.path.abspath(sys.argv[0]))

CONFIG_PATH = os.path.join(BASE_DIR, "config", "config.json")
SCHEMA_PATH = os.path.join(BASE_DIR, "config", "schemas.json")

def cargar_config() -> dict:
    with open(CONFIG_PATH, encoding="utf-8") as f:
        return json.load(f)

def cargar_schemas() -> dict:
    with open(SCHEMA_PATH, encoding="utf-8") as f:
        return json.load(f)

def calcular_hash_fila(row: dict, cols: List[str]) -> str:
    def norm(v):
        if v is None:
            return ""
        if isinstance(v, float):
            return str(int(v)) if v.is_integer() else str(v)
        return str(v).strip()
    parts = [norm(row.get(c)) for c in cols]
    s = "|".join(parts)
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def dbf_to_dataframe(dbf_path: str, columns: List[str] = None) -> pd.DataFrame:
    logging.info(f"Leyendo DBF: {dbf_path}")
    table = DBF(dbf_path, load=True, ignore_missing_memofile=True, char_decode_errors="ignore")
    df = pd.DataFrame(iter(table))
    if columns:
        lower = {c.lower(): c for c in df.columns}
        sel = [lower[c.lower()] for c in columns if c.lower() in lower]
        missing = [c for c in columns if c.lower() not in lower]
        if missing:
            logging.warning(f"Columnas no encontradas y excluidas: {missing}")
        df = df[sel]
    return df


def filter_new_or_changed(
    df: pd.DataFrame,
    engine,
    table_name: str,
    key_cols: List[str],
    hash_field: str,
    hash_cols: List[str]
) -> pd.DataFrame:
    """
    1) Calcula row_hash en Python.
    2) Elimina duplicados internos según key_cols.
    3) Carga TODO el mapping key->row_hash de MySQL.
    4) Filtra en Python solo las filas cuyo hash no coincida o no exista.
    """
    # 1) calcular SHA256
    df[hash_field] = [calcular_hash_fila(r, hash_cols) for r in df.to_dict("records")]

    # 2) dedupe interno
    df = df.drop_duplicates(subset=key_cols, keep="first")

    # 3) traer todo existing mapping
    meta = MetaData()
    tbl = Table(table_name, meta, autoload_with=engine)
    cols = [tbl.c[k] for k in key_cols] + [tbl.c[hash_field]]
    stmt = select(*cols)
    with engine.connect() as conn:
        rows = conn.execute(stmt).mappings().all()

    existing: dict[Tuple, str] = {
        tuple(row[k] for k in key_cols): row[hash_field]
        for row in rows
    }

    # 4) filtrar
    def is_changed(r):
        key = tuple(r[k] for k in key_cols)
        return existing.get(key) != r[hash_field]

    mask = df.apply(is_changed, axis=1)
    return df.loc[mask].copy()


def upsert_dataframe_con_progreso(
    df: pd.DataFrame,
    mysql_uri: str,
    table_name: str,
    key_cols: List[str],
    hash_field: str,
    chunk_size: int,
    progress_callback: Callable[[int], None]
):
    """
    Upsert por lotes con ON DUPLICATE KEY UPDATE.
    Asegura que row_hash se refresque en cada UPDATE.
    """
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
            if c.name not in key_cols and c.name in df.columns
        }
        # forzamos update de row_hash
        if hash_field in df.columns:
            upd[hash_field] = stmt.inserted[hash_field]

        stmt = stmt.on_duplicate_key_update(**upd)
        with engine.begin() as conn:
            conn.execute(stmt, chunk)
        progress_callback(int(((i + len(chunk)) / total) * 100))


def log_sync_history(
    mysql_uri: str,
    dbf_name: str,
    sync_time: datetime,
    rows_processed: int,
    rows_synced: int,
    mem_used_mb: float
):
    engine = create_engine(mysql_uri, connect_args={"charset": "utf8mb4"})
    stmt = text("""
      INSERT INTO etl_sync_log
        (dbf_name, sync_time, rows_processed, rows_inserted, mem_used_mb)
      VALUES (:dbf, :ts, :rp, :rs, :mem)
    """)
    with engine.begin() as conn:
        conn.execute(stmt, {
            "dbf": dbf_name,
            "ts": sync_time,
            "rp": rows_processed,
            "rs": rows_synced,
            "mem": mem_used_mb
        })


def ejecutar_etl_con_progreso(
    dbf_name: str,
    chunk_size: int,
    progress_callback: Callable[[int], None]
) -> str:
    start = time.time()
    cfg = cargar_config()
    schemas = cargar_schemas()
    entry = next(
        e for e in schemas["ENTRIES"]["CATALOGS"] + schemas["ENTRIES"]["TRANSACTIONAL"]
        if e["DBF"].lower() == dbf_name.lower()
    )

    engine = create_engine(cfg["MYSQL_URI"], connect_args={"charset": "utf8mb4"})
    key_cols = entry["TARGET"]["HASHES"]
    hash_field = "row_hash"
    hash_cols = key_cols

    # 1) leer DBF
    src = [c["SOURCE"] for c in entry["TARGET"]["COLUMNS"]]
    df = dbf_to_dataframe(os.path.join(cfg["DBF_DIR"], f"{dbf_name}.DBF"), src)
    rows_processed = len(df)

    # 2) rename SOURCE→TARGET
    lower = {c.lower(): c for c in df.columns}
    rename = {
        lower[c["SOURCE"].lower()]: c["TARGET"]
        for c in entry["TARGET"]["COLUMNS"]
        if c["SOURCE"].lower() in lower
    }
    df = df.rename(columns=rename)

    # 3) filtrar nuevos o cambiados
    df_to_sync = filter_new_or_changed(df, engine,
                                       entry["TARGET"]["TABLE"],
                                       key_cols, hash_field, hash_cols)
    rows_synced = len(df_to_sync)

    # 4) upsert
    upsert_dataframe_con_progreso(df_to_sync, cfg["MYSQL_URI"],
                                  entry["TARGET"]["TABLE"],
                                  key_cols, hash_field,
                                  chunk_size, progress_callback)

    # 5) log & fecha
    proc = psutil.Process()
    mem = proc.memory_info().rss / (1024 ** 2)
    now = datetime.now()
    log_sync_history(cfg["MYSQL_URI"], dbf_name, now,
                     rows_processed, rows_synced, mem)
    actualizar_fecha(dbf_name, now.isoformat(sep=" ", timespec="seconds"))

    return (f"Procesadas: {rows_processed}, "
            f"memoria: {mem:.2f} MB, duracion.: {time.time()-start:.2f}s.")
