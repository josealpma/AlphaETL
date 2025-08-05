# etl/control.py

import json
import os

from sqlalchemy import create_engine, text
from datetime import datetime

CONTROL_FILE = "config/sync_control.json"

def cargar_control():
    if not os.path.exists(CONTROL_FILE):
        return {}
    with open(CONTROL_FILE, "r", encoding="utf-8") as f:
        return json.load(f)

def guardar_control(data):
    with open(CONTROL_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4)

def obtener_ultima_fecha(nombre_dbf):
    control = cargar_control()
    return control.get(nombre_dbf, {}).get("ultima_fecha", None)

    
def obtener_ultima_fecha_db(nombre_dbf: str, mysql_uri: str) -> datetime | None:
    """
    Devuelve la última fecha de sincronización registrada en tbl_sync_log
    para el DBF indicado, o None si no hay registros.
    """
    engine = create_engine(mysql_uri, connect_args={"charset": "utf8mb4"})
    sql = text("""
        SELECT MAX(sync_time) AS ultima_fecha
          FROM tbl_sync_log
         WHERE dbf_name = :dbf
    """)
    with engine.connect() as conn:
        result = conn.execute(sql, {"dbf": nombre_dbf}).scalar()
    return result  # será un objeto datetime o None

def actualizar_fecha(nombre_dbf, nueva_fecha):
    control = cargar_control()
    if nombre_dbf not in control:
        control[nombre_dbf] = {}
    control[nombre_dbf]["ultima_fecha"] = nueva_fecha
    guardar_control(control)

def obtener_hashes(nombre_dbf):
    control = cargar_control()
    return control.get(nombre_dbf, {}).get("registros", {})

def actualizar_hashes(nombre_dbf, nuevos_hashes: dict):
    control = cargar_control()
    if nombre_dbf not in control:
        control[nombre_dbf] = {}
    control[nombre_dbf]["registros"] = nuevos_hashes
    guardar_control(control)
