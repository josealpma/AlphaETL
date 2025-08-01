
#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import json
import argparse
from dbfread import DBF
import pandas as pd
from sqlalchemy import (
    create_engine, MetaData, text, exc
)
from sqlalchemy.dialects.mysql import insert as mysql_insert, TEXT as MySQLText
from tqdm import tqdm
import numpy as np
from time import sleep


def load_config():
    """Carga configuración global desde config.json"""
    base = os.path.dirname(__file__)
    with open(os.path.join(base, 'config.json'), 'r', encoding='utf-8') as f:
        cfg = json.load(f)
    return cfg.get('DBF_DIR'), cfg.get('MYSQL_URI'), cfg.get('CHUNK_SIZE', 500)


def create_engine_with_retry(mysql_uri, retries=3, delay=5):
    for attempt in range(retries):
        try:
            engine = create_engine(
                mysql_uri,
                pool_pre_ping=True,
                pool_recycle=3600,
                pool_size=5,
                max_overflow=10,
                connect_args={
                    'connect_timeout': 30,
                    'read_timeout': 60,
                    'write_timeout': 60
                }
            )
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return engine
        except exc.SQLAlchemyError as e:
            if attempt == retries - 1:
                raise
            print(f"Error de conexión (intento {attempt+1}/{retries}): {e}")
            sleep(delay * (attempt+1))


def dbf_to_dataframe(path):
    """Lee un DBF completo en un DataFrame con saneamiento"""
    table = DBF(path, load=False, ignore_missing_memofile=True)
    if not table.field_names:
        raise ValueError(f"DBF vacío o sin campos: {path}")
    records = []
    for rec in table:
        clean = {
            k: v.decode('latin-1').strip() if isinstance(v, bytes) else v
            for k, v in rec.items() if not k.startswith('_')
        }
        records.append(clean)
    df = pd.DataFrame(records)
    if df.empty:
        raise ValueError(f"No hay registros en DBF: {path}")
    df.dropna(axis=1, how='all', inplace=True)
    return df


def upsert_or_replace(df, engine, table_name, chunk_size, force_replace=False):
    """
    Upsert por PK o replace completo. Si force_replace=True hace replace.
    """
    tbl = None
    meta = MetaData()
    tbl_lower = table_name.lower()
    # verificar existencia tabla
    try:
        meta.reflect(bind=engine, only=[tbl_lower])
        tbl = meta.tables.get(tbl_lower)
    except Exception:
        tbl = None

    # fuerza replace o tabla no existe
    if force_replace or tbl is None:
        df.to_sql(
            tbl_lower, engine,
            if_exists='replace', index=False,
            chunksize=chunk_size,
            dtype={col: MySQLText(collation='utf8mb4_unicode_ci')
                   for col in df.columns}
        )
        return

    # si tiene PK -> upsert
    pk_cols = [c.name for c in tbl.primary_key.columns]
    if pk_cols:
        stmt = mysql_insert(tbl).on_duplicate_key_update(
            **{c.name: mysql_insert(tbl).inserted[c.name]
               for c in tbl.columns if c.name not in pk_cols}
        )
        recs = df.to_dict(orient='records')
        with engine.begin() as conn:
            for i in range(0, len(recs), chunk_size):
                conn.execute(stmt, recs[i:i+chunk_size])
    else:
        df.to_sql(
            tbl_lower, engine,
            if_exists='append', index=False, chunksize=chunk_size
        )


def stream_etl(dbf_path, engine, table_name, chunk_size):
    """Procesa el DBF por lotes con barra de progreso y manejo de DataError."""
    df_iter = DBF(dbf_path, load=False, ignore_missing_memofile=True)
    total = len(df_iter)
    batch = []
    force_replace = False
    with tqdm(total=total, desc=table_name, unit='rec') as pbar:
        for rec in df_iter:
            clean = {
                k: v.decode('latin-1').strip() if isinstance(v, bytes) else v
                for k, v in rec.items() if not k.startswith('_')
            }
            batch.append(clean)
            if len(batch) >= chunk_size:
                df = pd.DataFrame(batch).replace({np.nan: None})
                try:
                    upsert_or_replace(df, engine, table_name, chunk_size, force_replace)
                except exc.DataError as e:
                    if 'Data too long' in str(e):
                        print(f"Overflow detectado en '{table_name}', recreando tabla con TEXT...")
                        force_replace = True
                        upsert_or_replace(df, engine, table_name, chunk_size, True)
                    else:
                        raise
                batch.clear()
                pbar.update(chunk_size)
        # último lote
        if batch:
            df = pd.DataFrame(batch).replace({np.nan: None})
            try:
                upsert_or_replace(df, engine, table_name, chunk_size, force_replace)
            except exc.DataError as e:
                if 'Data too long' in str(e):
                    print(f"Overflow detectado en '{table_name}' (último lote), recreando tabla con TEXT...")
                    upsert_or_replace(df, engine, table_name, chunk_size, True)
                else:
                    raise
            pbar.update(len(batch))


def main():
    DBF_DIR, MYSQL_URI, CHUNK_SIZE = load_config()
    p = argparse.ArgumentParser(description='ETL DBF→MySQL robusto')
    p.add_argument('--dbf', '-t', help='DBF (sin .dbf) a procesar')
    args = p.parse_args()

    engine = create_engine_with_retry(MYSQL_URI)
    files = [f for f in os.listdir(DBF_DIR)
             if f.lower().endswith('.dbf') and
             (not args.dbf or f.lower().startswith(args.dbf.lower()))]

    for fname in files:
        tbl = os.path.splitext(fname)[0]
        print(f"\n▼ Procesando '{tbl}'...")
        stream_etl(os.path.join(DBF_DIR, fname), engine, tbl, CHUNK_SIZE)
        cnt = engine.execute(text(f"SELECT COUNT(*) FROM `{tbl.lower()}`")).scalar()
        print(f"✔ '{tbl}' completado con {cnt} registros.")
    engine.dispose()

if __name__ == '__main__':
    main()
