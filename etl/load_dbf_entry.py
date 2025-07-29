# etl/load_dbf_entry.py

import os
import pandas as pd
from dbfread import DBF
from sqlalchemy import create_engine
import json

CONFIG_PATH = "config/config.json"

def cargar_config():
    with open(CONFIG_PATH, "r") as f:
        return json.load(f)

def dbf_to_dataframe(path, fields=None):
    table = DBF(path, encoding='latin1', load=True)
    df = pd.DataFrame(iter(table))
    if fields:
        df = df[fields]
    return df

def map_columns(df, columns_map):
    renamed = {}
    for col in columns_map:
        renamed[col["SOURCE"]] = col["TARGET"]
    return df.rename(columns=renamed)

def cargar_df_a_mysql(df, tabla_destino, mysql_uri):
    engine = create_engine(mysql_uri)
    with engine.begin() as conn:
        df.to_sql(tabla_destino, con=conn, if_exists="replace", index=False)
    print(f"Datos insertados en {tabla_destino} correctamente.")

def procesar_entry(entry, config):
    dbf_name = entry["DBF"]
    columnas = entry.get("COLUMNS")
    target_table = entry["TARGET"]["TABLE"]
    columnas_destino = entry["TARGET"]["COLUMNS"]

    dbf_path = os.path.join(config["DBF_DIR"], f"{dbf_name}.DBF")

    print(f"Extrayendo {dbf_name} desde {dbf_path}")
    df = dbf_to_dataframe(dbf_path, [col["SOURCE"] for col in columnas_destino])

    print(f"Renombrando columnas según mapeo → {target_table}")
    df = map_columns(df, columnas_destino)

    print(f"Insertando en base de datos → tabla `{target_table}`")
    cargar_df_a_mysql(df, target_table, config["MYSQL_URI"])

def ejecutar_vaciado_agentes():
    config = cargar_config()
    agentes_entry = next(e for e in config["ENTRIES"] if e["DBF"] == "AGENTES")
    procesar_entry(agentes_entry, config)

if __name__ == "__main__":
    ejecutar_vaciado_agentes()
