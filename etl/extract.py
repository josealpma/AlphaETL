# etl/extract.py

from dbfread import DBF
import pandas as pd
import os
from datetime import datetime

def leer_dbf_como_dataframe(ruta_dbf: str, campo_fecha: str = None, fecha_inicio: str = None, fecha_fin: str = None):
    """
    Carga un archivo DBF como DataFrame y aplica filtro por fechas si se indica.
    
    Args:
        ruta_dbf (str): Ruta al archivo .dbf
        campo_fecha (str): Nombre del campo de fecha para filtrar
        fecha_inicio (str): Fecha inicial (formato YYYY-MM-DD)
        fecha_fin (str): Fecha final (formato YYYY-MM-DD)
    
    Returns:
        pd.DataFrame: DataFrame con los datos cargados y filtrados (si aplica)
    """
    if not os.path.exists(ruta_dbf):
        raise FileNotFoundError(f"No se encontró el archivo DBF: {ruta_dbf}")

    tabla = DBF(ruta_dbf, load=True, encoding='latin1')
    df = pd.DataFrame(iter(tabla))

    if campo_fecha and fecha_inicio:
        df[campo_fecha] = pd.to_datetime(df[campo_fecha], errors='coerce')
        fecha_inicio = pd.to_datetime(fecha_inicio)
        fecha_fin = pd.to_datetime(fecha_fin) if fecha_fin else pd.Timestamp.today()
        df = df[(df[campo_fecha] >= fecha_inicio) & (df[campo_fecha] <= fecha_fin)]

    return df
