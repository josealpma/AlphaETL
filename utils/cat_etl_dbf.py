
#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import logging
import json
import threading
import time
import psutil
import tkinter as tk
from tkinter import ttk, messagebox
from dbfread import DBF
import pandas as pd
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.dialects.mysql import insert as mysql_insert

# Logging básico
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)

# Funciones ETL existentes
def dbf_to_dataframe(path, columns=None):
    basename = os.path.basename(path)
    logging.info(f"Leyendo DBF: {path}")
    table = DBF(path, load=True, ignore_missing_memofile=True, char_decode_errors='ignore')
    df = pd.DataFrame(iter(table))
    if columns:
        col_map = {col.lower(): col for col in df.columns}
        actual, missing = [], []
        for col in columns:
            key = col.lower()
            if key in col_map:
                actual.append(col_map[key])
            else:
                missing.append(col)
        if missing:
            logging.warning(f"Columnas no encontradas en {basename}: {missing}")
        df = df.loc[:, actual]
    return df


def upsert_dataframe(df, engine, table_name, key_columns, chunk_size):
    metadata = MetaData()
    table = Table(table_name, metadata, autoload_with=engine)
    records = df.to_dict(orient='records')
    with engine.begin() as conn:
        for i in range(0, len(records), chunk_size):
            chunk = records[i:i + chunk_size]
            stmt = mysql_insert(table)
            update_cols = {
                col.name: stmt.inserted[col.name]
                for col in table.columns
                if col.name not in key_columns and col.name in df.columns
            }
            stmt = stmt.on_duplicate_key_update(**update_cols)
            conn.execute(stmt, chunk)


def run_etl_for_entry(cfg, entry_name):
    # Inicializar monitoreo de recursos
    proc = psutil.Process(os.getpid())
    start_mem = proc.memory_info().rss
    start_cpu = proc.cpu_times()
    start_time = time.time()

    # Lectura de configuración
    dbf_dir = cfg.get('DBF_DIR', r"C:\alpha\dbf")
    mysql_uri = cfg['MYSQL_URI']
    chunk_size = cfg.get('CHUNK_SIZE', 1000)
    entries = cfg.get('ENTRIES', [])

    # Encontrar la entrada especificada
    entry = next((e for e in entries if e.get('DBF','').lower() == entry_name.lower()), None)
    if not entry:
        raise ValueError(f"Entry '{entry_name}' no encontrada en configuración")

    # Construir parámetros de ETL
    mappings = entry.get('TARGET', {}).get('COLUMNS', [])
    cols = [m['SOURCE'] for m in mappings] if mappings else entry.get('COLUMNS', [])
    dest_table = entry.get('TARGET', {}).get('TABLE', entry['DBF'])
    key_columns = entry.get('KEYS', [])

    dbf_file = os.path.join(dbf_dir, f"{entry['DBF']}.dbf")
    if not os.path.isfile(dbf_file):
        raise FileNotFoundError(f"No encontrado DBF: {dbf_file}")

    # Conectar a MySQL
    engine = create_engine(mysql_uri, connect_args={"charset": "utf8mb4"})

    # Leer y transformar datos
    df = dbf_to_dataframe(dbf_file, columns=cols)
    if mappings:
        col_map = {col.lower(): col for col in df.columns}
        rename_map = {}
        for m in mappings:
            src, tgt = m['SOURCE'], m['TARGET']
            key = src.lower()
            if key in col_map:
                rename_map[col_map[key]] = tgt
        df = df.rename(columns=rename_map)

    missing_keys = set(entry.get('KEYS', [])) - set(df.columns)
    if missing_keys:
        raise KeyError(f"Claves no encontradas: {missing_keys}")

    # Ejecutar upsert
    upsert_dataframe(df, engine, dest_table, key_columns, chunk_size)

    # Medir recursos al finalizar
    end_time = time.time()
    end_cpu = proc.cpu_times()
    end_mem = proc.memory_info().rss

    elapsed = end_time - start_time
    cpu_user = end_cpu.user - start_cpu.user
    cpu_system = end_cpu.system - start_cpu.system
    mem_diff = end_mem - start_mem

    logging.info(f"ETL '{entry_name}' completado en {elapsed:.2f}s | CPU (user: {cpu_user:.2f}s, sys: {cpu_system:.2f}s) | Memoria : {mem_diff/1024/1024:.2f} MB")


# Función para iniciar la interfaz gráfica
def launch_gui():
    # Ruta fija al config.json
    config_path = os.path.join(os.path.dirname(__file__), 'config.json')
    with open(config_path, 'r', encoding='utf-8') as f:
        cfg = json.load(f)

    entries = cfg.get('ENTRIES', [])
    dbf_list = [e.get('DBF') for e in entries]

    root = tk.Tk()
    root.title("ETL DBF→MySQL")
    root.geometry("350x150")

    lbl = ttk.Label(root, text="Seleccione el DBF a procesar:")
    lbl.pack(pady=(20, 5))

    combo = ttk.Combobox(root, values=dbf_list, state='readonly')
    combo.pack()
    if dbf_list:
        combo.current(0)

    def on_run():
        entry_name = combo.get()
        btn.config(state='disabled')
        def task():
            try:
                run_etl_for_entry(cfg, entry_name)
                messagebox.showinfo("Éxito", f"ETL para {entry_name} completado.")
            except Exception as e:
                messagebox.showerror("Error", str(e))
            finally:
                btn.config(state='normal')
        threading.Thread(target=task, daemon=True).start()

    btn = ttk.Button(root, text="Ejecutar ETL", command=on_run)
    btn.pack(pady=10)

    root.mainloop()

if __name__ == "__main__":
    launch_gui()
