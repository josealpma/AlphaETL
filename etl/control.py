# etl/control.py

import json
import os

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
